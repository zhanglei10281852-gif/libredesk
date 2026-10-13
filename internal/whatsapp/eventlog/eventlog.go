// Package eventlog durably records per-business-event processing progress for inbound
// WhatsApp webhook deliveries. Each inbound message, outbound status receipt and
// template-status fan-out target gets one stable-identity row, so worker reclaim,
// process restart and byte-identical Meta redeliveries only re-run non-terminal units.
package eventlog

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/abhinavxd/libredesk/internal/dbutil"
	"github.com/jmoiron/sqlx"
	"github.com/zerodha/logf"
)

//go:embed queries.sql
var efs embed.FS

// Kind discriminates the business event carried by a row.
type Kind string

const (
	KindMessage  Kind = "message"
	KindStatus   Kind = "status"
	KindTemplate Kind = "template"
)

// Status is the per-unit processing state. Succeeded/ignored/permanent are terminal.
type Status string

const (
	StatusPending    Status = "pending"
	StatusDeferred   Status = "deferred"
	StatusProcessing Status = "processing"
	StatusSucceeded  Status = "succeeded"
	StatusIgnored    Status = "ignored"
	StatusPermanent  Status = "permanent"
)

// Terminal reports whether no further processing of the unit is expected.
func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusIgnored || s == StatusPermanent
}

// ErrLeaseLost is returned when a state transition does not own the unit anymore,
// e.g. a stale lease was taken over while the original handler was still running.
var ErrLeaseLost = errors.New("whatsapp event lease lost")

// Event is one persisted business-event unit.
type Event struct {
	ID          int64           `db:"id"`
	CreatedAt   time.Time       `db:"created_at"`
	UpdatedAt   time.Time       `db:"updated_at"`
	ProcessedAt sql.NullTime    `db:"processed_at"`
	EventKey    string          `db:"event_key"`
	InboxID     int64           `db:"inbox_id"`
	Kind        Kind            `db:"kind"`
	Payload     json.RawMessage `db:"payload"`
	Status      Status          `db:"status"`
	Attempts    int             `db:"attempts"`
	LastError   sql.NullString  `db:"last_error"`
	LastErrorAt sql.NullTime    `db:"last_error_at"`
	NotBefore   sql.NullTime    `db:"not_before"`
	LeaseOwner  sql.NullString  `db:"lease_owner"`
	LeaseAt     sql.NullTime    `db:"lease_at"`
}

// Store persists inbound WhatsApp business-event progress.
type Store struct {
	db *sqlx.DB
	lo *logf.Logger
	q  queries
}

type queries struct {
	Ensure  *sqlx.Stmt `query:"ensure-whatsapp-event"`
	Get     *sqlx.Stmt `query:"get-whatsapp-event"`
	Claim   *sqlx.Stmt `query:"claim-whatsapp-event"`
	Succeed *sqlx.Stmt `query:"succeed-whatsapp-event"`
	Ignore  *sqlx.Stmt `query:"ignore-whatsapp-event"`
	PermFail *sqlx.Stmt `query:"permfail-whatsapp-event"`
	Retry   *sqlx.Stmt `query:"retry-whatsapp-event"`
	Defer   *sqlx.Stmt `query:"defer-whatsapp-event"`
	Prune   *sqlx.Stmt `query:"prune-terminal-whatsapp-events"`
}

// New prepares the progress store.
func New(lo *logf.Logger, db *sqlx.DB) (*Store, error) {
	s := &Store{db: db, lo: lo}
	if err := dbutil.ScanSQLFile("queries.sql", &s.q, db, efs); err != nil {
		return nil, err
	}
	return s, nil
}

// Ensure inserts a unit as pending or returns the existing row for its stable key.
func (s *Store) Ensure(ctx context.Context, inboxID int, kind Kind, eventKey string, payload json.RawMessage) (*Event, error) {
	var e Event
	if err := s.q.Ensure.GetContext(ctx, &e, eventKey, inboxID, string(kind), []byte(payload)); err != nil {
		return nil, fmt.Errorf("ensuring whatsapp event %s: %w", eventKey, err)
	}
	return &e, nil
}

// Get loads one unit by id.
func (s *Store) Get(ctx context.Context, id int64) (*Event, error) {
	var e Event
	if err := s.q.Get.GetContext(ctx, &e, id); err != nil {
		return nil, err
	}
	return &e, nil
}

// Claim atomically leases a non-terminal unit. The boolean is false when the unit is
// terminal, owned by another live worker, or waiting for its not_before.
func (s *Store) Claim(ctx context.Context, id int64, owner string, staleLease time.Duration) (*Event, bool, error) {
	var e Event
	err := s.q.Claim.GetContext(ctx, &e, id, owner, intervalArg(staleLease))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("claiming whatsapp event %d: %w", id, err)
	}
	return &e, true, nil
}

// MarkSucceeded records a completed unit. Only the current lease owner may transition it.
func (s *Store) MarkSucceeded(ctx context.Context, id int64, owner string) error {
	return s.transition(ctx, s.q.Succeed, id, owner)
}

// MarkIgnored records a terminal, deliberately skipped unit with a diagnostic reason.
func (s *Store) MarkIgnored(ctx context.Context, id int64, reason, owner string) error {
	return s.transition(ctx, s.q.Ignore, id, reason, owner)
}

// MarkPermanent records a terminal, permanently unprocessable unit with a diagnostic reason.
func (s *Store) MarkPermanent(ctx context.Context, id int64, reason, owner string) error {
	return s.transition(ctx, s.q.PermFail, id, reason, owner)
}

// MarkRetry releases the unit as pending after a transient failure.
func (s *Store) MarkRetry(ctx context.Context, id int64, reason, owner string) error {
	return s.transition(ctx, s.q.Retry, id, reason, owner)
}

// MarkDeferred parks the unit until notBefore, e.g. a status whose message row is missing.
func (s *Store) MarkDeferred(ctx context.Context, id int64, notBefore time.Time, reason, owner string) error {
	return s.transition(ctx, s.q.Defer, id, reason, notBefore, owner)
}

// Prune deletes terminal rows older than the retention windows.
func (s *Store) Prune(ctx context.Context, terminalRetention, permanentRetention time.Duration) (int64, error) {
	res, err := s.q.Prune.ExecContext(ctx, intervalArg(terminalRetention), intervalArg(permanentRetention))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (s *Store) transition(ctx context.Context, stmt *sqlx.Stmt, args ...any) error {
	res, err := stmt.ExecContext(ctx, args...)
	if err != nil {
		if id, ok := args[0].(int64); ok {
			return fmt.Errorf("updating whatsapp event %d: %w", id, err)
		}
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrLeaseLost
	}
	return nil
}

// intervalArg renders a duration as a Postgres interval text such as "600 seconds".
func intervalArg(d time.Duration) string {
	return fmt.Sprintf("%d seconds", int64(d.Seconds()))
}
