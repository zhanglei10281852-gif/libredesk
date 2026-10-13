// Package inbox provides functionality to manage inboxes in the system.
package inbox

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/abhinavxd/libredesk/internal/conversation/models"
	"github.com/abhinavxd/libredesk/internal/crypto"
	"github.com/abhinavxd/libredesk/internal/dbutil"
	"github.com/abhinavxd/libredesk/internal/envelope"
	imodels "github.com/abhinavxd/libredesk/internal/inbox/models"
	"github.com/abhinavxd/libredesk/internal/stringutil"
	umodels "github.com/abhinavxd/libredesk/internal/user/models"
	"github.com/jmoiron/sqlx"
	"github.com/knadh/go-i18n"
	"github.com/lib/pq"
	"github.com/volatiletech/null/v9"
	"github.com/zerodha/logf"
)

const (
	ChannelEmail    = "email"
	ChannelLiveChat = "livechat"
	ChannelWhatsApp = "whatsapp"
)

var (
	// Embedded filesystem
	//go:embed queries.sql
	efs embed.FS

	// ErrInboxNotFound is returned when an inbox is not found.
	ErrInboxNotFound = errors.New("inbox not found")
)

type initFn func(imodels.Inbox, MessageStore, UserStore) (Inbox, error)

// Closer provides a function for closing an inbox.
type Closer interface {
	Close() error
}

// Identifier provides a method for obtaining a unique identifier for the inbox.
type Identifier interface {
	Identifier() int
}

// MessageHandler defines methods for handling message operations.
type MessageHandler interface {
	Receive(context.Context) error
	Send(models.OutboundMessage) error
}

// Inbox combines the operations of an inbox including its lifecycle, identification, and message handling.
type Inbox interface {
	Closer
	Identifier
	MessageHandler
	Name() string
	FromAddress() string
	FromNameTemplate() string
	ReplyToAddress() string
	Channel() string
}

// EmailInbox exposes the addresses an email inbox owns and alias send verification.
type EmailInbox interface {
	Inbox
	PrimaryAddress() string
	StartAliasVerification(string, string) error
}

// MessageStore defines methods for storing and processing messages.
type MessageStore interface {
	MessageExists(string) (bool, error)
	EnqueueIncoming(models.IncomingMessage) error
}

// UserStore defines methods for fetching user information.
type UserStore interface {
	GetAgent(id int, email string) (umodels.User, error)
	IsEmailBlocked(email string) (bool, error)
}

type aliasVerificationState struct {
	Email      string     `db:"email"`
	Status     string     `db:"verification_status"`
	VerifiedAt *time.Time `db:"verified_at"`
}

// Opts contains the options for initializing the inbox manager.
type Opts struct {
	QueueSize   int
	Concurrency int
}

// receiverState tracks a *single*s inbox receiver goroutine.
type receiverState struct {
	cancel context.CancelFunc
	done   chan struct{} // closed when the goroutine exits
}

type Manager struct {
	mu            sync.RWMutex
	queries       queries
	inboxes       map[int]Inbox
	runtimeGen    map[int]int64 // committed generation of the unique active runtime
	lo            *logf.Logger
	i18n          *i18n.I18n
	receivers     map[int]receiverState
	msgStore      MessageStore
	usrStore      UserStore
	wg            sync.WaitGroup
	encryptionKey string
	db            *sqlx.DB

	// switchLocks serializes every runtime-affecting operation per inbox so
	// generations of one inbox can never commit or hand over out of order.
	switchLocksMu sync.Mutex
	switchLocks   map[int]*sync.Mutex
}

// Prepared queries.
type queries struct {
	GetInbox               *sqlx.Stmt `query:"get-inbox"`
	GetInboxByUUID         *sqlx.Stmt `query:"get-inbox-by-uuid"`
	GetActive              *sqlx.Stmt `query:"get-active-inboxes"`
	GetAll                 *sqlx.Stmt `query:"get-all-inboxes"`
	Update                 *sqlx.Stmt `query:"update"`
	BumpGeneration         *sqlx.Stmt `query:"bump-runtime-generation"`
	Toggle                 *sqlx.Stmt `query:"toggle"`
	SoftDelete             *sqlx.Stmt `query:"soft-delete"`
	InsertInbox            *sqlx.Stmt `query:"insert-inbox"`
	UpdateConfig           *sqlx.Stmt `query:"update-config"`
	LockInbox              *sqlx.Stmt `query:"lock-inbox"`
	ResetAliasVerification *sqlx.Stmt `query:"reset-alias-verification"`
	DeleteAddresses        *sqlx.Stmt `query:"delete-inbox-email-addresses"`
	InsertAddress          *sqlx.Stmt `query:"insert-inbox-email-address"`

	GetAliasVerificationStates   *sqlx.Stmt `query:"get-alias-verification-states"`
	StartAliasVerification       *sqlx.Stmt `query:"start-alias-verification"`
	FailAliasVerification        *sqlx.Stmt `query:"fail-alias-verification"`
	CompleteAliasVerification    *sqlx.Stmt `query:"complete-alias-verification"`
	FailAliasVerificationByToken *sqlx.Stmt `query:"fail-alias-verification-by-token"`
	ExpireAliasVerifications     *sqlx.Stmt `query:"expire-alias-verifications"`
}

// New returns a new inbox manager.
func New(lo *logf.Logger, db *sqlx.DB, i18n *i18n.I18n, encryptionKey string) (*Manager, error) {
	var q queries
	if err := dbutil.ScanSQLFile("queries.sql", &q, db, efs); err != nil {
		return nil, err
	}

	m := &Manager{
		lo:            lo,
		inboxes:       make(map[int]Inbox),
		runtimeGen:    make(map[int]int64),
		receivers:     make(map[int]receiverState),
		switchLocks:   make(map[int]*sync.Mutex),
		queries:       q,
		i18n:          i18n,
		encryptionKey: encryptionKey,
		db:            db,
	}
	return m, nil
}

// lockInbox serializes runtime-affecting operations (update, enable, disable,
// delete, reload) for a single inbox. Different inboxes stay independent. The
// returned function releases the lock.
func (m *Manager) lockInbox(id int) func() {
	m.switchLocksMu.Lock()
	l, ok := m.switchLocks[id]
	if !ok {
		l = &sync.Mutex{}
		m.switchLocks[id] = l
	}
	m.switchLocksMu.Unlock()
	l.Lock()
	return l.Unlock
}

// SetMessageStore sets the message store for the manager.
func (m *Manager) SetMessageStore(store MessageStore) {
	m.msgStore = store
}

// SetUserStore sets the user store for the manager.
func (m *Manager) SetUserStore(store UserStore) {
	m.usrStore = store
}

// Register registers the inbox with the manager.
func (m *Manager) Register(i Inbox) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inboxes[i.Identifier()] = i
}

// Get retrieves the initialized inbox instance with the specified ID from memory.
func (m *Manager) Get(id int) (Inbox, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	i, ok := m.inboxes[id]
	if !ok {
		return nil, ErrInboxNotFound
	}
	return i, nil
}

// GetDBRecord returns the inbox record from the DB by numeric ID or UUID.
// If the identifier contains a dash, it's treated as a UUID; otherwise as a numeric ID.
func (m *Manager) GetDBRecord(identifier any) (imodels.Inbox, error) {
	var inbox imodels.Inbox

	// If it's a string with dashes, look up by UUID; otherwise by numeric ID.
	str := fmt.Sprintf("%v", identifier)
	if strings.Contains(str, "-") {
		if err := m.queries.GetInboxByUUID.Get(&inbox, str); err != nil {
			if err == sql.ErrNoRows {
				return inbox, envelope.NewError(envelope.InputError, m.i18n.T("validation.notFoundInbox"), nil)
			}
			m.lo.Error("error fetching inbox", "error", err)
			return inbox, envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
		}
	} else {
		id, err := strconv.Atoi(str)
		if err != nil {
			return inbox, envelope.NewError(envelope.InputError, m.i18n.T("validation.notFoundInbox"), nil)
		}
		if err := m.queries.GetInbox.Get(&inbox, id); err != nil {
			if err == sql.ErrNoRows {
				return inbox, envelope.NewError(envelope.InputError, m.i18n.T("validation.notFoundInbox"), nil)
			}
			m.lo.Error("error fetching inbox", "error", err)
			return inbox, envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
		}
	}

	decryptedConfig, err := m.decryptInboxConfig(inbox.Config)
	if err != nil {
		m.lo.Error("error decrypting inbox config", "identifier", identifier, "error", err)
		return imodels.Inbox{}, envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
	}
	inbox.Config = decryptedConfig

	m.decryptInboxSecret(&inbox)

	return inbox, nil
}

// GetAll returns all inboxes from the DB.
func (m *Manager) GetAll() ([]imodels.Inbox, error) {
	var inboxes = make([]imodels.Inbox, 0)
	if err := m.queries.GetAll.Select(&inboxes); err != nil {
		m.lo.Error("error fetching inboxes", "error", err)
		return nil, envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
	}

	// Decrypt sensitive fields in each inbox config
	for i := range inboxes {
		decryptedConfig, err := m.decryptInboxConfig(inboxes[i].Config)
		if err != nil {
			m.lo.Error("error decrypting inbox config", "id", inboxes[i].ID, "error", err)
			return nil, envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
		}
		inboxes[i].Config = decryptedConfig

		// Decrypt secret field
		m.decryptInboxSecret(&inboxes[i])
	}

	return inboxes, nil
}

// Create creates an inbox in the DB.
func (m *Manager) Create(inbox imodels.Inbox) (imodels.Inbox, error) {
	var primary string
	if inbox.Channel == ChannelEmail {
		var err error
		if primary, inbox.Aliases, err = m.normalizeEmailAddresses(inbox.From, inbox.Aliases); err != nil {
			return imodels.Inbox{}, err
		}
		for i := range inbox.Aliases {
			inbox.Aliases[i].VerificationStatus = imodels.AliasVerificationNotVerified
			inbox.Aliases[i].VerifiedAt = nil
		}
	}
	if inbox.Channel == ChannelLiveChat {
		secret := inbox.Secret.String
		if secret == "" {
			generated, err := stringutil.RandomAlphanumeric(32)
			if err != nil {
				return imodels.Inbox{}, fmt.Errorf("generating inbox secret: %w", err)
			}
			secret = generated
		}
		encryptedSecret, err := crypto.Encrypt(secret, m.encryptionKey)
		if err != nil {
			return imodels.Inbox{}, fmt.Errorf("encrypting inbox secret: %w", err)
		}
		inbox.Secret = null.StringFrom(encryptedSecret)
	}

	// Encrypt sensitive fields before saving
	encryptedConfig, err := m.encryptInboxConfig(inbox.Config)
	if err != nil {
		m.lo.Error("error encrypting inbox config", "error", err)
		return imodels.Inbox{}, envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
	}

	tx, err := m.db.Beginx()
	if err != nil {
		return imodels.Inbox{}, m.persistenceError("starting inbox creation", err)
	}
	defer func() { _ = tx.Rollback() }()

	var createdInbox imodels.Inbox
	if err := tx.Stmtx(m.queries.InsertInbox).Get(&createdInbox, inbox.Channel, encryptedConfig, inbox.Name, inbox.From, inbox.Enabled, inbox.CSATEnabled, inbox.PromptTagsOnReply, inbox.ReopenWindowHours, inbox.Secret, inbox.LinkedEmailInboxID, inbox.FromNameTemplate); err != nil {
		m.lo.Error("error creating inbox", "error", err)
		return imodels.Inbox{}, m.persistenceError("creating inbox", err)
	}
	if inbox.Channel == ChannelEmail {
		if err := m.insertEmailAddresses(tx, createdInbox.ID, primary, inbox.Aliases); err != nil {
			return imodels.Inbox{}, err
		}
		createdInbox.Aliases = inbox.Aliases
	}
	if err := tx.Commit(); err != nil {
		return imodels.Inbox{}, m.persistenceError("committing inbox creation", err)
	}

	// Decrypt before returning
	decryptedConfig, err := m.decryptInboxConfig(createdInbox.Config)
	if err != nil {
		m.lo.Error("error decrypting inbox config after creation", "error", err)
	} else {
		createdInbox.Config = decryptedConfig
	}

	// Decrypt secret field
	m.decryptInboxSecret(&createdInbox)

	return createdInbox, nil
}

// InitInboxes initializes and registers active inboxes with the manager.
func (m *Manager) InitInboxes(initFn initFn) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inboxRecords, err := m.getActive()
	if err != nil {
		m.lo.Error("error fetching active inboxes", "error", err)
		return fmt.Errorf("fetching active inboxes: %v", err)
	}

	for _, inboxRecord := range inboxRecords {
		inbox, err := initFn(inboxRecord, m.msgStore, m.usrStore)
		if err != nil {
			m.lo.Error("error initializing inbox",
				"name", inboxRecord.Name,
				"channel", inboxRecord.Channel,
				"error", err)
			continue
		}
		m.inboxes[inbox.Identifier()] = inbox
		// Resume tracking the generation persisted in the DB so the first
		// update after a restart can only publish a strictly newer generation.
		m.runtimeGen[inbox.Identifier()] = inboxRecord.RuntimeGeneration
	}
	return nil
}

// StartInbox constructs and starts the runtime for a freshly created inbox.
// Operations for the inbox are serialized; only one receiver is ever started.
func (m *Manager) StartInbox(ctx context.Context, id int, initFn initFn) error {
	defer m.lockInbox(id)()
	return m.activateRecord(ctx, id, initFn)
}

// ReloadInbox rebuilds the runtime of a single inbox from the persisted record
// (OAuth reconnect, externally applied config changes). The candidate is built
// while the old runtime keeps serving and takes over at a single handoff point;
// a disabled or deleted record stops the runtime instead. Serialized per inbox.
func (m *Manager) ReloadInbox(ctx context.Context, id int, initFn initFn) error {
	defer m.lockInbox(id)()
	return m.activateRecord(ctx, id, initFn)
}

// activateRecord loads the persisted record and activates or stops the runtime.
// Caller must hold the inbox's switch lock.
func (m *Manager) activateRecord(ctx context.Context, id int, initFn initFn) error {
	record, err := m.GetDBRecord(id)
	if err != nil {
		// The record is gone (deleted): make sure no runtime survives it.
		m.stopRuntime(id)
		return nil
	}
	return m.activate(ctx, record, initFn)
}

// activate builds the candidate runtime from record while any old runtime keeps
// serving, bumps the DB generation, and hands over. A disabled record stops the
// runtime. Caller must hold the inbox's switch lock.
func (m *Manager) activate(ctx context.Context, record imodels.Inbox, initFn initFn) error {
	id := record.ID
	if !record.Enabled {
		m.stopRuntime(id)
		return nil
	}

	candidate, err := initFn(record, m.msgStore, m.usrStore)
	if err != nil {
		if candidate != nil {
			// Release everything the failed constructor already created.
			_ = candidate.Close()
		}
		return m.runtimeInitError(id, err)
	}

	var gen int64
	if err := m.queries.BumpGeneration.Get(&gen, id); err != nil {
		_ = candidate.Close()
		if err == sql.ErrNoRows {
			// The inbox was deleted while the candidate was being built.
			m.stopRuntime(id)
			return nil
		}
		return m.persistenceError("bumping inbox runtime generation", err)
	}
	return m.handoff(ctx, candidate, gen)
}

// handoff is the single controlled takeover point for one inbox generation:
// the old receiver is cancelled and fully awaited BEFORE the new object is
// published and its receiver started, so two receivers never consume in
// parallel. The replaced object is closed exactly once. A candidate belonging
// to an older generation than the committed one is discarded and closed.
// Caller must hold the inbox's switch lock.
func (m *Manager) handoff(ctx context.Context, candidate Inbox, gen int64) error {
	id := candidate.Identifier()

	m.mu.Lock()
	if current := m.runtimeGen[id]; current >= gen {
		m.mu.Unlock()
		m.lo.Warn("discarding stale inbox runtime generation", "id", id, "committed_generation", current, "candidate_generation", gen)
		_ = candidate.Close()
		return envelope.NewError(envelope.ConflictError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
	}
	rs, hadReceiver := m.receivers[id]
	if hadReceiver {
		rs.cancel()
		delete(m.receivers, id)
	}
	m.mu.Unlock()

	// Wait outside the lock so the old receiver goroutine can finish. The old
	// object stays registered and keeps serving sends during this wait.
	if hadReceiver {
		<-rs.done
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Re-check while awaiting the old receiver: a newer commit must win.
	if current := m.runtimeGen[id]; current >= gen {
		_ = candidate.Close()
		m.lo.Warn("discarding stale inbox runtime generation after receiver drain", "id", id, "committed_generation", current, "candidate_generation", gen)
		return envelope.NewError(envelope.ConflictError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
	}

	old := m.inboxes[id]
	m.inboxes[id] = candidate
	m.runtimeGen[id] = gen
	m.startReceiver(ctx, candidate)

	if old != nil {
		if err := old.Close(); err != nil {
			m.lo.Error("error closing replaced inbox runtime", "id", id, "error", err)
		}
	}
	return nil
}

// stopRuntime cancels and waits for the receiver, then removes and closes the
// runtime object exactly once. It is the intentional stop path for disabled
// and deleted inboxes. Caller must hold the inbox's switch lock.
func (m *Manager) stopRuntime(id int) {
	m.mu.Lock()
	rs, hadReceiver := m.receivers[id]
	if hadReceiver {
		rs.cancel()
		delete(m.receivers, id)
	}
	old, hadInbox := m.inboxes[id]
	if hadInbox {
		delete(m.inboxes, id)
	}
	delete(m.runtimeGen, id)
	m.mu.Unlock()

	// Wait outside the lock so the receiver goroutine can finish.
	if hadReceiver {
		<-rs.done
	}
	if hadInbox {
		if err := old.Close(); err != nil {
			m.lo.Error("error closing stopped inbox runtime", "id", id, "error", err)
		}
	}
}

// runtimeInitError turns a candidate constructor failure into an actionable
// error for the administrator; the pre-save state is left untouched.
func (m *Manager) runtimeInitError(id int, err error) error {
	m.lo.Error("inbox runtime candidate failed, keeping previous runtime", "id", id, "error", err)
	return envelope.NewError(envelope.InputError, m.i18n.Ts("inbox.runtimeInitFailed", "error", err.Error()), nil)
}

// Update updates an inbox in the DB and hot-swaps its runtime as one
// per-inbox serialized configuration generation:
//
//   - While the old runtime keeps serving, the replacement candidate is
//     constructed and verified. On failure the candidate's partial resources
//     are closed, the transaction is rolled back and an actionable error is
//     returned; persisted config, aliases, secret placeholders, the old
//     receiver and the old sender all remain at their pre-save state.
//   - Only once the candidate is usable does the same generation commit in the
//     DB, after which the runtime is replaced at one controlled handoff point
//     (stop old receiver -> publish new object -> start new receiver).
//
// When initFn is nil, only persistence is performed (no runtime takeover).
func (m *Manager) Update(ctx context.Context, id int, inbox imodels.Inbox, initFn initFn) (updated imodels.Inbox, err error) {
	defer m.lockInbox(id)()

	tx, err := m.db.Beginx()
	if err != nil {
		return imodels.Inbox{}, m.persistenceError("starting inbox update", err)
	}
	defer func() { _ = tx.Rollback() }()
	var lockedID int
	if err := tx.Stmtx(m.queries.LockInbox).Get(&lockedID, id); err != nil {
		return imodels.Inbox{}, m.persistenceError("locking inbox", err)
	}
	var current imodels.Inbox
	if err := tx.Stmtx(m.queries.GetInbox).Get(&current, id); err != nil {
		return imodels.Inbox{}, m.persistenceError("fetching inbox", err)
	}
	current.Config, err = m.decryptInboxConfig(current.Config)
	if err != nil {
		return imodels.Inbox{}, m.persistenceError("decrypting inbox config", err)
	}
	if inbox.Channel != current.Channel {
		return imodels.Inbox{}, envelope.NewError(envelope.InputError, m.i18n.T("globals.messages.badRequest"), nil)
	}
	var primary string
	if current.Channel == ChannelEmail {
		if inbox.Aliases == nil {
			inbox.Aliases = current.Aliases
		}
		if primary, inbox.Aliases, err = m.normalizeEmailAddresses(inbox.From, inbox.Aliases); err != nil {
			return imodels.Inbox{}, err
		}
	}

	// Preserve existing passwords if update has empty password
	switch current.Channel {
	case "email":
		var currentCfg struct {
			AuthType             string            `json:"auth_type"`
			OAuth                map[string]string `json:"oauth"`
			IMAP                 []map[string]any  `json:"imap"`
			SMTP                 []map[string]any  `json:"smtp"`
			ReplyTo              string            `json:"reply_to"`
			EnablePlusAddressing bool              `json:"enable_plus_addressing"`
		}
		var updateCfg struct {
			AuthType             string            `json:"auth_type"`
			OAuth                map[string]string `json:"oauth"`
			IMAP                 []map[string]any  `json:"imap"`
			SMTP                 []map[string]any  `json:"smtp"`
			ReplyTo              string            `json:"reply_to"`
			EnablePlusAddressing bool              `json:"enable_plus_addressing"`
		}

		if err := json.Unmarshal(current.Config, &currentCfg); err != nil {
			m.lo.Error("error unmarshalling current config", "id", id, "error", err)
			return imodels.Inbox{}, envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
		}
		if len(inbox.Config) == 0 {
			return imodels.Inbox{}, envelope.NewError(envelope.InputError, m.i18n.Ts("globals.messages.empty", "name", "{globals.terms.config}"), nil)
		}
		if err := json.Unmarshal(inbox.Config, &updateCfg); err != nil {
			m.lo.Error("error unmarshalling update config", "id", id, "error", err)
			return imodels.Inbox{}, envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
		}

		if len(updateCfg.IMAP) == 0 {
			return imodels.Inbox{}, envelope.NewError(envelope.InputError, m.i18n.T("inbox.emptyIMAP"), nil)
		}

		if len(updateCfg.SMTP) == 0 {
			return imodels.Inbox{}, envelope.NewError(envelope.InputError, m.i18n.T("inbox.emptySMTP"), nil)
		}

		// Preserve existing IMAP passwords if update has empty password
		for i := range updateCfg.IMAP {
			if updateCfg.IMAP[i]["password"] == "" && i < len(currentCfg.IMAP) {
				updateCfg.IMAP[i]["password"] = currentCfg.IMAP[i]["password"]
			}
		}

		// Preserve existing SMTP passwords if update has empty password
		for i := range updateCfg.SMTP {
			if updateCfg.SMTP[i]["password"] == "" && i < len(currentCfg.SMTP) {
				updateCfg.SMTP[i]["password"] = currentCfg.SMTP[i]["password"]
			}
		}

		// Preserve existing OAuth fields if update has empty
		if currentCfg.OAuth != nil {
			if updateCfg.OAuth == nil {
				updateCfg.OAuth = make(map[string]string)
			}
			for k, v := range currentCfg.OAuth {
				if updateCfg.OAuth[k] == "" {
					updateCfg.OAuth[k] = v
				}
			}
		}

		updatedConfig, err := json.Marshal(updateCfg)
		if err != nil {
			m.lo.Error("error marshalling updated config", "id", id, "error", err)
			return imodels.Inbox{}, err
		}
		inbox.Config = updatedConfig
	case "livechat":
		// Preserve existing secret if update contains password dummy
		if inbox.Secret.Valid && strings.Contains(inbox.Secret.String, stringutil.PasswordDummy) {
			inbox.Secret = current.Secret
		} else if inbox.Secret.Valid && inbox.Secret.String != "" {
			// Encrypt new secret
			encryptedSecret, err := crypto.Encrypt(inbox.Secret.String, m.encryptionKey)
			if err != nil {
				return imodels.Inbox{}, fmt.Errorf("encrypting inbox secret: %w", err)
			}
			inbox.Secret = null.StringFrom(encryptedSecret)
		}
	case ChannelWhatsApp:
		merged, err := m.MergeWhatsAppSecrets(current.Config, inbox.Config)
		if err != nil {
			return imodels.Inbox{}, err
		}
		inbox.Config = merged
	}

	// Encrypt sensitive fields before updating
	encryptedConfig, err := m.encryptInboxConfig(inbox.Config)
	if err != nil {
		m.lo.Error("error encrypting inbox config", "error", err)
		return imodels.Inbox{}, envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
	}

	// Build and verify the replacement runtime BEFORE committing, while the old
	// runtime keeps serving. The candidate is not registered and has no
	// receiver yet; on failure it is closed and the commit never happens.
	var candidate Inbox
	if initFn != nil && inbox.Enabled {
		candidateRecord := current
		candidateRecord.Channel = current.Channel
		candidateRecord.Config = inbox.Config
		candidateRecord.Name = inbox.Name
		candidateRecord.From = inbox.From
		candidateRecord.FromNameTemplate = inbox.FromNameTemplate
		candidateRecord.CSATEnabled = inbox.CSATEnabled
		candidateRecord.PromptTagsOnReply = inbox.PromptTagsOnReply
		candidateRecord.ReopenWindowHours = inbox.ReopenWindowHours
		candidateRecord.Enabled = inbox.Enabled
		candidateRecord.Secret = inbox.Secret
		candidateRecord.LinkedEmailInboxID = inbox.LinkedEmailInboxID
		candidateRecord.Aliases = inbox.Aliases

		candidate, err = initFn(candidateRecord, m.msgStore, m.usrStore)
		if err != nil {
			if candidate != nil {
				_ = candidate.Close()
			}
			return imodels.Inbox{}, m.runtimeInitError(id, err)
		}
	}

	var updatedInbox imodels.Inbox
	if err := tx.Stmtx(m.queries.Update).Get(&updatedInbox, id, inbox.Channel, encryptedConfig, inbox.Name, inbox.From, inbox.CSATEnabled, inbox.PromptTagsOnReply, inbox.ReopenWindowHours, inbox.Enabled, inbox.Secret, inbox.LinkedEmailInboxID, inbox.FromNameTemplate); err != nil {
		m.lo.Error("error updating inbox", "error", err)
		return imodels.Inbox{}, m.persistenceError("updating inbox", err)
	}
	if current.Channel == ChannelEmail {
		addresses := []string{primary}
		for _, alias := range inbox.Aliases {
			addresses = append(addresses, alias.Email)
		}
		if _, err := tx.Stmtx(m.queries.DeleteAddresses).Exec(id, pq.Array(addresses), primary); err != nil {
			return imodels.Inbox{}, m.persistenceError("removing inbox addresses", err)
		}
		if err := m.insertEmailAddresses(tx, id, primary, inbox.Aliases); err != nil {
			return imodels.Inbox{}, err
		}
		var currentConfig, updatedConfig imodels.Config
		if err := json.Unmarshal(current.Config, &currentConfig); err != nil {
			return imodels.Inbox{}, m.persistenceError("reading current email config", err)
		}
		if err := json.Unmarshal(inbox.Config, &updatedConfig); err != nil {
			return imodels.Inbox{}, m.persistenceError("reading updated email config", err)
		}
		if emailSendingConfigChanged(currentConfig, updatedConfig) {
			if _, err := tx.Stmtx(m.queries.ResetAliasVerification).Exec(id); err != nil {
				return imodels.Inbox{}, m.persistenceError("resetting alias verification", err)
			}
		}
		var states []aliasVerificationState
		if err := tx.Stmtx(m.queries.GetAliasVerificationStates).Select(&states, id); err != nil {
			return imodels.Inbox{}, m.persistenceError("fetching alias verification state", err)
		}
		for i := range inbox.Aliases {
			for _, state := range states {
				if state.Email == inbox.Aliases[i].Email {
					inbox.Aliases[i].VerificationStatus = state.Status
					inbox.Aliases[i].VerifiedAt = state.VerifiedAt
					break
				}
			}
		}
		updatedInbox.Aliases = inbox.Aliases
	}
	if err := tx.Commit(); err != nil {
		if candidate != nil {
			_ = candidate.Close()
		}
		return imodels.Inbox{}, m.persistenceError("committing inbox update", err)
	}

	// The committed generation now takes over the runtime at the single
	// controlled handoff point. Disabling intentionally stops the runtime.
	if initFn != nil {
		if inbox.Enabled {
			if err := m.handoff(ctx, candidate, updatedInbox.RuntimeGeneration); err != nil {
				return imodels.Inbox{}, err
			}
		} else {
			m.stopRuntime(id)
		}
	}

	// Decrypt before returning
	decryptedConfig, err := m.decryptInboxConfig(updatedInbox.Config)
	if err != nil {
		m.lo.Error("error decrypting inbox config after update", "error", err)
	} else {
		updatedInbox.Config = decryptedConfig
	}

	// Decrypt secret field
	m.decryptInboxSecret(&updatedInbox)

	return updatedInbox, nil
}

// MergeWhatsAppSecrets restores masked or empty secret fields in an update config from the currently stored config.
func (m *Manager) MergeWhatsAppSecrets(current, update json.RawMessage) (json.RawMessage, error) {
	var currentCfg, updateCfg map[string]any
	if err := json.Unmarshal(current, &currentCfg); err != nil {
		m.lo.Error("error unmarshalling current whatsapp config", "error", err)
		return nil, envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
	}
	if len(update) == 0 {
		return nil, envelope.NewError(envelope.InputError, m.i18n.Ts("globals.messages.empty", "name", "{globals.terms.config}"), nil)
	}
	if err := json.Unmarshal(update, &updateCfg); err != nil {
		m.lo.Error("error unmarshalling whatsapp update config", "error", err)
		return nil, envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
	}
	for _, fieldName := range []string{"access_token", "app_secret", "webhook_verify_token"} {
		val, _ := updateCfg[fieldName].(string)
		if val == "" || strings.Contains(val, stringutil.PasswordDummy) {
			if existing, ok := currentCfg[fieldName].(string); ok {
				updateCfg[fieldName] = existing
			}
		}
	}
	merged, err := json.Marshal(updateCfg)
	if err != nil {
		m.lo.Error("error marshalling whatsapp merged config", "error", err)
		return nil, err
	}
	return merged, nil
}

// Toggle flips the enabled state of an inbox in the DB and intentionally stops
// the runtime when disabling or builds and starts a single new receiver when
// enabling. Serialized per inbox.
func (m *Manager) Toggle(ctx context.Context, id int, initFn initFn) (imodels.Inbox, error) {
	defer m.lockInbox(id)()

	if _, err := m.queries.Toggle.ExecContext(ctx, id); err != nil {
		m.lo.Error("error toggling inbox", "error", err)
		return imodels.Inbox{}, envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
	}

	record, err := m.GetDBRecord(id)
	if err != nil {
		return imodels.Inbox{}, err
	}

	if initFn != nil {
		if err := m.activate(ctx, record, initFn); err != nil {
			return imodels.Inbox{}, err
		}
	}
	return record, nil
}

// SoftDelete soft deletes an inbox in the DB and intentionally stops its
// runtime. Serialized per inbox.
func (m *Manager) SoftDelete(id int) error {
	defer m.lockInbox(id)()

	if _, err := m.queries.SoftDelete.Exec(id); err != nil {
		m.lo.Error("error deleting inbox", "error", err)
		return envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
	}
	m.stopRuntime(id)
	return nil
}

func (m *Manager) normalizeEmailAddresses(from string, aliases imodels.EmailAliases) (string, imodels.EmailAliases, error) {
	primary, normalized, err := ValidateEmailAddresses(from, aliases)
	switch {
	case err == nil:
		return primary, normalized, nil
	case errors.Is(err, ErrDuplicateAddress):
		return "", nil, envelope.NewError(envelope.InputError, m.i18n.T("globals.messages.errorAlreadyExists"), nil)
	case errors.Is(err, ErrInvalidAliasAddress):
		return "", nil, envelope.NewError(envelope.InputError, m.i18n.T("validation.invalidEmail"), nil)
	default:
		return "", nil, envelope.NewError(envelope.InputError, m.i18n.T("validation.invalidFromAddress"), nil)
	}
}

// StartAliasVerification emails a token from the alias to the inbox's reply-to address, else its primary address.
func (m *Manager) StartAliasVerification(ctx context.Context, id int, address string) error {
	normalized, err := NormalizeEmailAddress(address)
	if err != nil {
		return envelope.NewError(envelope.InputError, m.i18n.T("validation.invalidEmail"), nil)
	}
	inbox, err := m.GetDBRecord(id)
	if err != nil {
		return err
	}
	idx := slices.IndexFunc(inbox.Aliases, func(alias imodels.EmailAlias) bool { return strings.EqualFold(alias.Email, normalized) })
	if idx < 0 {
		return envelope.NewError(envelope.InputError, m.i18n.T("admin.inbox.aliases.saveBeforeVerify"), nil)
	}
	runtimeInbox, err := m.Get(id)
	if err != nil {
		return envelope.NewError(envelope.InputError, m.i18n.T("status.disabledInbox"), nil)
	}
	emailInbox, ok := runtimeInbox.(EmailInbox)
	if !ok {
		m.lo.Error("inbox does not support alias verification", "inbox_id", id)
		return envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
	}
	token, err := stringutil.RandomAlphanumeric(48)
	if err != nil {
		m.lo.Error("error generating alias verification token", "inbox_id", id, "error", err)
		return envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
	}
	if _, err := m.queries.StartAliasVerification.ExecContext(ctx, id, normalized, imodels.AliasVerificationPending, token); err != nil {
		return m.persistenceError("starting alias verification", err)
	}
	go func() {
		if err := emailInbox.StartAliasVerification(normalized, token); err != nil {
			m.lo.Error("error sending alias verification email", "inbox_id", id, "alias", normalized, "error", err)
			if _, err := m.queries.FailAliasVerification.Exec(id, normalized, imodels.AliasVerificationFailed, token); err != nil {
				m.lo.Error("error marking alias verification failed", "inbox_id", id, "alias", normalized, "error", err)
			}
		}
	}()
	return nil
}

// CompleteAliasVerification consumes a verification message received by IMAP.
func (m *Manager) CompleteAliasVerification(ctx context.Context, id int, token, from string) error {
	normalized, normalizeErr := NormalizeEmailAddress(from)
	if normalizeErr == nil {
		result, err := m.queries.CompleteAliasVerification.ExecContext(ctx, id, token, normalized, imodels.AliasVerificationVerified)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected > 0 {
			return nil
		}
	}
	if _, err := m.queries.FailAliasVerificationByToken.ExecContext(ctx, id, token, imodels.AliasVerificationFailed, imodels.AliasVerificationPending); err != nil {
		return err
	}
	return normalizeErr
}

// ExpireAliasVerifications marks verifications still pending from before the given time as failed.
func (m *Manager) ExpireAliasVerifications(ctx context.Context, id int, startedBefore time.Time) error {
	_, err := m.queries.ExpireAliasVerifications.ExecContext(ctx, id, imodels.AliasVerificationFailed, imodels.AliasVerificationPending, startedBefore)
	return err
}

func (m *Manager) insertEmailAddresses(tx *sqlx.Tx, inboxID int, primary string, aliases imodels.EmailAliases) error {
	if _, err := tx.Stmtx(m.queries.InsertAddress).Exec(inboxID, primary, "primary", 0 /** position **/, imodels.AliasVerificationVerified); err != nil {
		return m.persistenceError("claiming inbox address", err)
	}
	for position, alias := range aliases {
		if _, err := tx.Stmtx(m.queries.InsertAddress).Exec(inboxID, alias.Email, "alias", position+1, imodels.AliasVerificationNotVerified); err != nil {
			return m.persistenceError("claiming inbox address", err)
		}
	}
	return nil
}

func (m *Manager) persistenceError(action string, err error) error {
	m.lo.Error("inbox persistence error", "action", action, "error", err)
	return envelope.NewError(envelope.GeneralError, m.i18n.T("globals.messages.somethingWentWrong"), nil)
}

// UpdateConfig updates only the config field of an inbox in the DB.
func (m *Manager) UpdateConfig(id int, config json.RawMessage) error {
	// Encrypt fields before updating
	encryptedConfig, err := m.encryptInboxConfig(config)
	if err != nil {
		m.lo.Error("error encrypting inbox config", "id", id, "error", err)
		return fmt.Errorf("encrypting inbox config: %w", err)
	}

	if _, err := m.queries.UpdateConfig.Exec(id, encryptedConfig); err != nil {
		m.lo.Error("error updating inbox config", "id", id, "error", err)
		return fmt.Errorf("updating inbox config: %w", err)
	}
	return nil
}

// CloseLiveChatClients disconnects widget websocket clients and returns the number of inboxes closed.
func (m *Manager) CloseLiveChatClients() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var n int
	for _, inb := range m.inboxes {
		if inb.Channel() != ChannelLiveChat {
			continue
		}
		if err := inb.Close(); err != nil {
			m.lo.Error("error closing livechat inbox", "error", err)
			continue
		}
		n++
	}
	return n
}

// startReceiver starts a receiver goroutine for the given inbox.
// Caller must hold m.mu.
func (m *Manager) startReceiver(ctx context.Context, inb Inbox) {
	done := make(chan struct{})
	receiverCtx, cancel := context.WithCancel(ctx)
	m.receivers[inb.Identifier()] = receiverState{cancel: cancel, done: done}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer close(done)
		if err := inb.Receive(receiverCtx); err != nil {
			m.lo.Error("error starting inbox receiver", "error", err)
		}
	}()
}

// Start starts the receiver for each inbox.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, inb := range m.inboxes {
		m.startReceiver(ctx, inb)
	}
	return nil
}

// Close closes all inboxes.
func (m *Manager) Close() {
	m.mu.Lock()

	// Cancel all receivers.
	for _, rs := range m.receivers {
		rs.cancel()
	}

	// Close all inboxes.
	for _, inb := range m.inboxes {
		inb.Close()
	}
	m.mu.Unlock()

	// Wait for all receiver goroutines to finish.
	m.wg.Wait()
}

// getActive returns all active inboxes from the DB.
func (m *Manager) getActive() ([]imodels.Inbox, error) {
	var inboxes []imodels.Inbox
	if err := m.queries.GetActive.Select(&inboxes); err != nil {
		m.lo.Error("fetching active inboxes", "error", err)
		return nil, err
	}

	// Decrypt sensitive fields in each inbox config
	for i := range inboxes {
		decryptedConfig, err := m.decryptInboxConfig(inboxes[i].Config)
		if err != nil {
			m.lo.Error("error decrypting inbox config", "id", inboxes[i].ID, "error", err)
			return nil, fmt.Errorf("decrypting inbox config for ID %d: %w", inboxes[i].ID, err)
		}
		inboxes[i].Config = decryptedConfig

		// Decrypt secret field
		m.decryptInboxSecret(&inboxes[i])
	}

	return inboxes, nil
}

// encryptInboxConfig encrypts sensitive fields in the inbox config JSON.
func (m *Manager) encryptInboxConfig(config json.RawMessage) (json.RawMessage, error) {
	if len(config) == 0 {
		return config, nil
	}

	var cfg map[string]any
	if err := json.Unmarshal(config, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshalling config: %w", err)
	}

	// Encrypt SMTP passwords
	if smtpSlice, ok := cfg["smtp"].([]any); ok {
		for i, smtpItem := range smtpSlice {
			if smtpMap, ok := smtpItem.(map[string]any); ok {
				if password, ok := smtpMap["password"].(string); ok && password != "" {
					encrypted, err := crypto.Encrypt(password, m.encryptionKey)
					if err != nil {
						return nil, fmt.Errorf("encrypting SMTP password at index %d: %w", i, err)
					}
					smtpMap["password"] = encrypted
				}
			}
		}
	}

	// Encrypt IMAP passwords
	if imapSlice, ok := cfg["imap"].([]any); ok {
		for i, imapItem := range imapSlice {
			if imapMap, ok := imapItem.(map[string]any); ok {
				if password, ok := imapMap["password"].(string); ok && password != "" {
					encrypted, err := crypto.Encrypt(password, m.encryptionKey)
					if err != nil {
						return nil, fmt.Errorf("encrypting IMAP password at index %d: %w", i, err)
					}
					imapMap["password"] = encrypted
				}
			}
		}
	}

	// Encrypt OAuth fields if present
	if oauthMap, ok := cfg["oauth"].(map[string]any); ok {
		fields := []string{"client_secret", "access_token", "refresh_token"}
		for _, fieldName := range fields {
			if fieldValue, ok := oauthMap[fieldName].(string); ok && fieldValue != "" {
				encrypted, err := crypto.Encrypt(fieldValue, m.encryptionKey)
				if err != nil {
					return nil, fmt.Errorf("encrypting OAuth %s: %w", fieldName, err)
				}
				oauthMap[fieldName] = encrypted
			}
		}
	}

	for _, fieldName := range []string{"access_token", "app_secret", "webhook_verify_token"} {
		if value, ok := cfg[fieldName].(string); ok && value != "" && !crypto.IsEncrypted(value) {
			encrypted, err := crypto.Encrypt(value, m.encryptionKey)
			if err != nil {
				return nil, fmt.Errorf("encrypting whatsapp %s: %w", fieldName, err)
			}
			cfg[fieldName] = encrypted
		}
	}

	encrypted, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshalling encrypted config: %w", err)
	}

	return encrypted, nil
}

// Decrypt failures clear the field so the app stays usable across encryption_key rotation.
func (m *Manager) decryptInboxConfig(config json.RawMessage) (json.RawMessage, error) {
	if len(config) == 0 {
		return config, nil
	}

	var cfg map[string]any
	if err := json.Unmarshal(config, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshalling config: %w", err)
	}

	if smtpSlice, ok := cfg["smtp"].([]any); ok {
		for i, smtpItem := range smtpSlice {
			if smtpMap, ok := smtpItem.(map[string]any); ok {
				if password, ok := smtpMap["password"].(string); ok && password != "" {
					decrypted, err := crypto.Decrypt(password, m.encryptionKey)
					if err != nil {
						m.lo.Error("error decrypting SMTP password, clearing field", "index", i, "error", err)
						smtpMap["password"] = ""
						continue
					}
					smtpMap["password"] = decrypted
				}
			}
		}
	}

	if imapSlice, ok := cfg["imap"].([]any); ok {
		for i, imapItem := range imapSlice {
			if imapMap, ok := imapItem.(map[string]any); ok {
				if password, ok := imapMap["password"].(string); ok && password != "" {
					decrypted, err := crypto.Decrypt(password, m.encryptionKey)
					if err != nil {
						m.lo.Error("error decrypting IMAP password, clearing field", "index", i, "error", err)
						imapMap["password"] = ""
						continue
					}
					imapMap["password"] = decrypted
				}
			}
		}
	}

	if oauthMap, ok := cfg["oauth"].(map[string]any); ok {
		fields := []string{"client_secret", "access_token", "refresh_token"}
		for _, fieldName := range fields {
			if fieldValue, ok := oauthMap[fieldName].(string); ok && fieldValue != "" {
				decrypted, err := crypto.Decrypt(fieldValue, m.encryptionKey)
				if err != nil {
					m.lo.Error("error decrypting OAuth field, clearing field", "field", fieldName, "error", err)
					oauthMap[fieldName] = ""
					continue
				}
				oauthMap[fieldName] = decrypted
			}
		}
	}

	for _, fieldName := range []string{"access_token", "app_secret", "webhook_verify_token"} {
		if value, ok := cfg[fieldName].(string); ok && crypto.IsEncrypted(value) {
			decrypted, err := crypto.Decrypt(value, m.encryptionKey)
			if err != nil {
				m.lo.Error("error decrypting whatsapp credential, clearing field", "field", fieldName, "error", err)
				cfg[fieldName] = ""
				continue
			}
			cfg[fieldName] = decrypted
		}
	}

	decrypted, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshalling decrypted config: %w", err)
	}

	return decrypted, nil
}

// decryptInboxSecret decrypts the inbox secret field if present.
func (m *Manager) decryptInboxSecret(inbox *imodels.Inbox) {
	if inbox.Secret.Valid && inbox.Secret.String != "" {
		decrypted, err := crypto.Decrypt(inbox.Secret.String, m.encryptionKey)
		if err != nil {
			m.lo.Error("error decrypting inbox secret", "inbox_id", inbox.ID, "error", err)
			return
		}
		inbox.Secret = null.StringFrom(decrypted)
	}
}

func emailSendingConfigChanged(current, updated imodels.Config) bool {
	if (current.AuthType == imodels.AuthTypeOAuth2) != (updated.AuthType == imodels.AuthTypeOAuth2) {
		return true
	}
	if !slices.EqualFunc(current.SMTP, updated.SMTP, func(a, b imodels.SMTPConfig) bool {
		return strings.EqualFold(a.Host, b.Host) && a.Port == b.Port &&
			a.Username == b.Username && a.Password == b.Password && a.AuthProtocol == b.AuthProtocol
	}) {
		return true
	}
	if updated.AuthType != imodels.AuthTypeOAuth2 {
		return false
	}
	if current.OAuth == nil || updated.OAuth == nil {
		return current.OAuth != updated.OAuth
	}
	a, b := current.OAuth, updated.OAuth
	return a.Provider != b.Provider || a.ClientID != b.ClientID || a.ClientSecret != b.ClientSecret ||
		a.TenantID != b.TenantID || a.RefreshToken != b.RefreshToken
}
