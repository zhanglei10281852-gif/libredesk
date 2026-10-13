package conversation

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/abhinavxd/libredesk/internal/conversation/models"
	"github.com/abhinavxd/libredesk/internal/dbutil"
	"github.com/abhinavxd/libredesk/internal/inbox"
	"github.com/abhinavxd/libredesk/internal/inbox/channel/livechat"
	whatsappChannel "github.com/abhinavxd/libredesk/internal/inbox/channel/whatsapp"
	imodels "github.com/abhinavxd/libredesk/internal/inbox/models"
	mmodels "github.com/abhinavxd/libredesk/internal/media/models"
	slaModels "github.com/abhinavxd/libredesk/internal/sla/models"
	"github.com/abhinavxd/libredesk/internal/template"
	"github.com/abhinavxd/libredesk/internal/testutil"
	"github.com/abhinavxd/libredesk/internal/user"
	wmodels "github.com/abhinavxd/libredesk/internal/webhook/models"
	"github.com/abhinavxd/libredesk/internal/ws"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/zerodha/logf"
)

// expireLease back-dates a message lease so recovery/re-claim logic sees it
// as dead without sleeping.
func expireLease(t *testing.T, db *sqlx.DB, id int) {
	t.Helper()
	if _, err := db.Exec(`UPDATE conversation_messages SET send_lease_until = NOW() - INTERVAL '1 minute' WHERE id = $1`, id); err != nil {
		t.Fatalf("expiring lease: %v", err)
	}
}

func messageStatus(t *testing.T, db *sqlx.DB, id int) (status string, gen int64, sourceID, owner *string) {
	t.Helper()
	if err := db.QueryRow(`SELECT status, send_generation, source_id, send_owner FROM conversation_messages WHERE id = $1`, id).
		Scan(&status, &gen, &sourceID, &owner); err != nil {
		t.Fatalf("selecting message: %v", err)
	}
	return
}

// A live lease is exclusive: only one owner per generation, and a later
// message in the same conversation cannot jump the queue.
func TestClaimOutgoingMessageIsExclusiveAndOrdered(t *testing.T) {
	db := testutil.NewDB(t, "dispatch_claim")
	var q queries
	if err := dbutil.ScanSQLFile("queries.sql", &q, db, efs); err != nil {
		t.Fatalf("preparing queries: %v", err)
	}
	conversationA, conversationB, senderID := seedOutgoingOrderTest(t, db)
	firstA := insertPendingMessage(t, db, conversationA, senderID, "a1")
	secondA := insertPendingMessage(t, db, conversationA, senderID, "a2")
	firstB := insertPendingMessage(t, db, conversationB, senderID, "b1")

	var claimed models.Message
	if err := q.ClaimOutgoingMessage.Get(&claimed, firstA, "owner-A", outgoingLeaseInterval); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if claimed.SendOwner.String != "owner-A" || !claimed.SendLeaseUntil.Valid || claimed.SendGeneration != 1 {
		t.Fatalf("unexpected claim row: %+v", claimed)
	}

	// Another instance cannot take the same live generation.
	if err := q.ClaimOutgoingMessage.Get(new(models.Message), firstA, "owner-B", outgoingLeaseInterval); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected live lease to block a second owner, got %v", err)
	}

	// Same conversation: the second message is blocked while the first is pending.
	if err := q.ClaimOutgoingMessage.Get(new(models.Message), secondA, "owner-A", outgoingLeaseInterval); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected earlier pending message to block later claim, got %v", err)
	}

	// Different conversations stay parallel.
	if err := q.ClaimOutgoingMessage.Get(&models.Message{}, firstB, "owner-B", outgoingLeaseInterval); err != nil {
		t.Fatalf("other conversations must claim in parallel: %v", err)
	}

	// Once finalized, the next message in the conversation is claimable.
	if _, err := db.Exec(`UPDATE conversation_messages SET status = 'sent', send_owner = NULL, send_lease_until = NULL WHERE id = $1`, firstA); err != nil {
		t.Fatalf("finalizing first: %v", err)
	}
	if err := q.ClaimOutgoingMessage.Get(&claimed, secondA, "owner-A2", outgoingLeaseInterval); err != nil {
		t.Fatalf("second message should claim after first finalized: %v", err)
	}
}

// A lease that expires before the channel was called is safely re-leasable to
// a new owner of the same generation.
func TestExpiredLeaseBeforeDispatchIsReclaimable(t *testing.T) {
	db := testutil.NewDB(t, "dispatch_reclaim")
	var q queries
	if err := dbutil.ScanSQLFile("queries.sql", &q, db, efs); err != nil {
		t.Fatalf("preparing queries: %v", err)
	}
	convA, _, senderID := seedOutgoingOrderTest(t, db)
	id := insertPendingMessage(t, db, convA, senderID, "x")

	if err := q.ClaimOutgoingMessage.Get(&models.Message{}, id, "dead-owner", outgoingLeaseInterval); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	expireLease(t, db, id)

	// send_started_at is still NULL: the dead owner never reached the channel.
	var claimed models.Message
	if err := q.ClaimOutgoingMessage.Get(&claimed, id, "new-owner", outgoingLeaseInterval); err != nil {
		t.Fatalf("new owner must take over an untouched expired lease: %v", err)
	}
	if claimed.SendOwner.String != "new-owner" || claimed.SendGeneration != 1 {
		t.Fatalf("generation must be preserved with new owner, got %+v", claimed)
	}
}

// After dispatch-start, an expired lease cannot be re-sent: recovery parks the
// row as unknown and it blocks the rest of the conversation.
func TestInterruptedDispatchBecomesUnknownAndBlocks(t *testing.T) {
	db := testutil.NewDB(t, "dispatch_unknown")
	var q queries
	if err := dbutil.ScanSQLFile("queries.sql", &q, db, efs); err != nil {
		t.Fatalf("preparing queries: %v", err)
	}
	convA, convB, senderID := seedOutgoingOrderTest(t, db)
	first := insertPendingMessage(t, db, convA, senderID, "a1")
	second := insertPendingMessage(t, db, convA, senderID, "a2")
	other := insertPendingMessage(t, db, convB, senderID, "b1")

	if err := q.ClaimOutgoingMessage.Get(&models.Message{}, first, "dead-owner", outgoingLeaseInterval); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if res, err := q.MarkOutgoingDispatchStarted.Exec(uuidForID(t, db, first), int64(1), "dead-owner", outgoingLeaseInterval); err != nil {
		t.Fatalf("mark started: %v", err)
	} else if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("expected marker to apply")
	}
	expireLease(t, db, first)

	// Re-claim must refuse: the channel may already have accepted it.
	if err := q.ClaimOutgoingMessage.Get(new(models.Message), first, "new-owner", outgoingLeaseInterval); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("an interrupted dispatch must never be re-claimed for resend, got %v", err)
	}

	// Recovery moves it to unknown.
	patch := []byte(`{"send_unknown_at":"2026-01-01T00:00:00Z","send_unknown_reason":"dispatch_interrupted"}`)
	var rows []terminalMessageRow
	if err := q.RecoverInterruptedDispatches.Select(&rows, patch); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if len(rows) != 1 || rows[0].Status != models.MessageStatusUnknown {
		t.Fatalf("expected one unknown row, got %+v", rows)
	}

	// The unknown head blocks the second message in the same conversation, but
	// other conversations are unaffected.
	pending := []models.Message{}
	if err := q.GetOutgoingPendingMessages.Select(&pending, pq.Array([]int{})); err != nil {
		t.Fatalf("peek: %v", err)
	}
	gotIDs := map[int]bool{}
	for _, msg := range pending {
		gotIDs[msg.ID] = true
	}
	if gotIDs[first] || gotIDs[second] {
		t.Fatalf("unknown head and its successor must not be dispatched, got %v", gotIDs)
	}
	if !gotIDs[other] {
		t.Fatalf("other conversations must stay dispatchable, got %v", gotIDs)
	}
}

// The terminal CAS only accepts the current generation's current owner while
// the row is still pending.
func TestCommitTerminalIsOwnerAndGenerationGated(t *testing.T) {
	db := testutil.NewDB(t, "dispatch_commit")
	var q queries
	if err := dbutil.ScanSQLFile("queries.sql", &q, db, efs); err != nil {
		t.Fatalf("preparing queries: %v", err)
	}
	convA, _, senderID := seedOutgoingOrderTest(t, db)
	id := insertPendingMessage(t, db, convA, senderID, "x")
	messageUUID := uuidForID(t, db, id)

	claim := func(owner string) {
		t.Helper()
		if err := q.ClaimOutgoingMessage.Get(&models.Message{}, id, owner, outgoingLeaseInterval); err != nil {
			t.Fatalf("claim %s: %v", owner, err)
		}
	}
	commit := func(gen int64, owner, status string) int64 {
		t.Helper()
		res, err := q.CommitOutgoingTerminal.Exec(messageUUID, gen, owner, status, "", []byte("{}"))
		if err != nil {
			t.Fatalf("commit gen=%d owner=%s: %v", gen, owner, err)
		}
		n, _ := res.RowsAffected()
		return n
	}

	claim("owner-A")
	if n := commit(2, "owner-A", models.MessageStatusSent); n != 0 {
		t.Fatalf("a foreign generation must not finalize")
	}
	if n := commit(1, "owner-B", models.MessageStatusSent); n != 0 {
		t.Fatalf("a stale owner must not finalize")
	}
	if n := commit(1, "owner-A", models.MessageStatusFailed); n != 1 {
		t.Fatalf("current owner must finalize exactly once")
	}
	if n := commit(1, "owner-A", models.MessageStatusSent); n != 0 {
		t.Fatalf("a finalized row must not move again")
	}
	status, gen, _, owner := messageStatus(t, db, id)
	if status != models.MessageStatusFailed || gen != 1 || owner != nil {
		t.Fatalf("unexpected row after commit: status=%s gen=%d owner=%v", status, gen, owner)
	}
}

// Manual retry starts a new generation and clears exactly what WhatsApp allows
// clearing (wamid + provider receipt state + unknown markers).
func TestManualRetryStartsNewGeneration(t *testing.T) {
	db := testutil.NewDB(t, "dispatch_retry")
	var q queries
	if err := dbutil.ScanSQLFile("queries.sql", &q, db, efs); err != nil {
		t.Fatalf("preparing queries: %v", err)
	}
	convA, _, senderID := seedOutgoingOrderTest(t, db)
	id := insertPendingMessage(t, db, convA, senderID, "x")
	messageUUID := uuidForID(t, db, id)

	// Simulate a dead dispatch: started, leased, wamid stored, then unknown.
	if _, err := db.Exec(`
		UPDATE conversation_messages
		SET status = 'unknown',
		    source_id = 'wamid.OLD',
		    send_generation = 1,
		    send_owner = 'dead',
		    send_lease_until = NOW(),
		    send_started_at = NOW(),
		    meta = meta || jsonb_build_object(
		      'provider_status','read',
		      'provider_failure_reason','old',
		      'send_unknown_reason','dispatch_interrupted',
		      'send_unknown_at','2026-01-01T00:00:00Z')
		WHERE id = $1`, id); err != nil {
		t.Fatalf("seeding unknown row: %v", err)
	}

	var row struct {
		UUID           string `db:"uuid"`
		SendGeneration int64  `db:"send_generation"`
	}
	if err := q.MarkMessagePendingForRetry.Get(&row, messageUUID); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if row.SendGeneration != 2 {
		t.Fatalf("retry must bump generation, got %d", row.SendGeneration)
	}

	var status, metaStr string
	var gen int64
	var sourceID, owner *string
	var started, lease *time.Time
	if err := db.QueryRow(`SELECT status, send_generation, COALESCE(meta::text,'{}'), source_id, send_owner, send_started_at, send_lease_until FROM conversation_messages WHERE id = $1`, id).
		Scan(&status, &gen, &metaStr, &sourceID, &owner, &started, &lease); err != nil {
		t.Fatalf("selecting retried row: %v", err)
	}
	if status != "pending" || gen != 2 || sourceID != nil || owner != nil || started != nil || lease != nil {
		t.Fatalf("retried row not reset: status=%s gen=%d source=%v owner=%v started=%v lease=%v", status, gen, sourceID, owner, started, lease)
	}
	var meta map[string]any
	if err := json.Unmarshal([]byte(metaStr), &meta); err != nil {
		t.Fatalf("meta: %v", err)
	}
	for _, key := range []string{"provider_status", "provider_failure_reason", "send_unknown_reason", "send_unknown_at"} {
		if _, ok := meta[key]; ok {
			t.Fatalf("retry must clear %s, meta=%s", key, metaStr)
		}
	}

	// A late old-generation callback cannot repoint the new attempt.
	if res, err := q.UpdateMessageSourceIDByUUID.Exec(messageUUID, "wamid.LATE_OLD", int64(1)); err != nil {
		t.Fatalf("old gen source update: %v", err)
	} else if n, _ := res.RowsAffected(); n != 0 {
		t.Fatalf("old generation callback must not write")
	}
	// The current generation can.
	if res, err := q.UpdateMessageSourceIDByUUID.Exec(messageUUID, "wamid.NEW", int64(2)); err != nil {
		t.Fatalf("new gen source update: %v", err)
	} else if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("new generation callback must write")
	}
}

// WhatsApp receipts converge an in-flight or unknown row exactly once and
// never resurrect a failed message.
func TestWhatsAppReceiptConvergence(t *testing.T) {
	db := testutil.NewDB(t, "dispatch_receipts")
	var q queries
	if err := dbutil.ScanSQLFile("queries.sql", &q, db, efs); err != nil {
		t.Fatalf("preparing queries: %v", err)
	}
	convA, _, senderID := seedOutgoingOrderTest(t, db)

	seed := func(id *int, status string) string {
		*id = insertPendingMessage(t, db, convA, senderID, "m")
		u := uuidForID(t, db, *id)
		if _, err := db.Exec(`UPDATE conversation_messages SET status = $2, source_id = $3 WHERE id = $1`, *id, status, "wamid-"+u[:8]); err != nil {
			t.Fatalf("seeding: %v", err)
		}
		return u
	}

	patch := func(status string) []byte {
		b, _ := json.Marshal(map[string]any{"provider_status": status, "provider_status_updated_at": "2026-01-01T00:00:00Z"})
		return b
	}
	apply := func(sourceID, dbStatus, providerStatus string) (string, bool) {
		t.Helper()
		var row struct {
			terminalMessageRow
			OldStatus string `db:"old_status"`
			Converged bool   `db:"converged"`
		}
		if err := q.ApplyWhatsAppMessageStatus.Get(&row, sourceID, dbStatus, patch(providerStatus)); err != nil {
			t.Fatalf("apply %s: %v", providerStatus, err)
		}
		return row.Status, row.Converged
	}

	// Unknown row converges on a delivered receipt (proof of acceptance).
	var unknownID int
	unknownUUID := seed(&unknownID, models.MessageStatusUnknown)
	status, converged := apply("wamid-"+unknownUUID[:8], models.MessageStatusSent, WhatsAppStatusDelivered)
	if status != models.MessageStatusSent || !converged {
		t.Fatalf("unknown should converge to sent once, status=%s converged=%v", status, converged)
	}
	if _, converged = apply("wamid-"+unknownUUID[:8], models.MessageStatusSent, WhatsAppStatusRead); converged {
		t.Fatalf("a second receipt must not converge again")
	}

	// In-flight (pending) row converges on a failed receipt and stays failed.
	var pendingID int
	pendingUUID := seed(&pendingID, models.MessageStatusPending)
	status, converged = apply("wamid-"+pendingUUID[:8], models.MessageStatusFailed, WhatsAppStatusFailed)
	if status != models.MessageStatusFailed || !converged {
		t.Fatalf("pending should converge to failed once, status=%s converged=%v", status, converged)
	}
	if status, _ = apply("wamid-"+pendingUUID[:8], models.MessageStatusSent, WhatsAppStatusSent); status != models.MessageStatusFailed {
		t.Fatalf("failure is sticky, got %s", status)
	}
}

// ---------------------------------------------------------------------------
// Manager-level dispatch behavior with a fake channel.
// ---------------------------------------------------------------------------

type dispatchMedia struct{ mediaStore }

func (dispatchMedia) GetByModel(int, string) ([]mmodels.Media, error) { return nil, nil }

type dispatchSLA struct{}

func (dispatchSLA) ApplySLA(time.Time, int, int, int) (slaModels.SLAPolicy, error) {
	return slaModels.SLAPolicy{}, nil
}
func (dispatchSLA) CreateNextResponseSLAEvent(int, int, int, int) (time.Time, error) {
	return time.Time{}, nil
}
func (dispatchSLA) SetLatestSLAEventMetAt(int, string) (time.Time, error) {
	return time.Time{}, nil
}
func (dispatchSLA) EvaluateConversationSLA(int) error { return nil }

type dispatchWebhook struct {
	updated int
}

func (d *dispatchWebhook) TriggerEvent(event wmodels.WebhookEvent, _ any) {
	if event == wmodels.EventMessageUpdated {
		d.updated++
	}
}
func (d *dispatchWebhook) TriggerWebhook(int, wmodels.WebhookEvent, any) {}

type dispatchFakeInbox struct {
	inbox.Inbox
	id      int
	channel string
	sendErr error
	calls   int
	lastGen int64
}

func (f *dispatchFakeInbox) Identifier() int                                  { return f.id }
func (f *dispatchFakeInbox) Name() string                                     { return "fake" }
func (f *dispatchFakeInbox) FromAddress() string                              { return "" }
func (f *dispatchFakeInbox) FromNameTemplate() string                         { return "" }
func (f *dispatchFakeInbox) ReplyToAddress() string                           { return "" }
func (f *dispatchFakeInbox) Channel() string                                  { return f.channel }
func (f *dispatchFakeInbox) Close() error                                     { return nil }
func (f *dispatchFakeInbox) Receive(context.Context) error                    { return nil }
func (f *dispatchFakeInbox) Send(m models.OutboundMessage) error {
	f.calls++
	f.lastGen = m.SendGeneration
	return f.sendErr
}

type dispatchFakeInboxStore struct{ inb inbox.Inbox }

func (s *dispatchFakeInboxStore) Get(int) (inbox.Inbox, error) { return s.inb, nil }
func (s *dispatchFakeInboxStore) GetDBRecord(any) (imodels.Inbox, error) {
	return imodels.Inbox{Channel: inbox.ChannelLiveChat, Enabled: true}, nil
}
func (s *dispatchFakeInboxStore) GetAll() ([]imodels.Inbox, error) { return nil, nil }

func newDispatchManager(t *testing.T, db *sqlx.DB, inb *dispatchFakeInbox) (*Manager, *dispatchWebhook) {
	t.Helper()
	lo := logf.New(logf.Opts{Level: logf.FatalLevel})
	lang := testutil.NewI18n(t)
	users, err := user.New(lang, user.Opts{DB: db, Lo: &lo})
	if err != nil {
		t.Fatal(err)
	}
	wh := &dispatchWebhook{}
	m := &Manager{
		db:           db,
		lo:           &lo,
		i18n:         lang,
		inboxStore:   &dispatchFakeInboxStore{inb: inb},
		userStore:    users,
		mediaStore:   dispatchMedia{},
		webhookStore: wh,
		wsHub:        ws.NewHub(&lo, nil),
		slaStore:     dispatchSLA{},
		template:     &template.Manager{},
	}
	if err := dbutil.ScanSQLFile("queries.sql", &m.q, db, efs); err != nil {
		t.Fatal(err)
	}
	return m, wh
}

func insertDispatchMessage(t *testing.T, db *sqlx.DB, conversationID, senderID int) int {
	t.Helper()
	var id int
	if err := db.QueryRow(`
		INSERT INTO conversation_messages ("type", status, conversation_id, content, text_content, content_type, sender_id, sender_type, private, meta)
		VALUES ('outgoing', 'pending', $1, 'hi', 'hi', 'text', $2, 'agent', false, '{"is_automated":true}')
		RETURNING id`, conversationID, senderID).Scan(&id); err != nil {
		t.Fatalf("inserting message: %v", err)
	}
	return id
}

func TestSendOutgoingMessageOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		dbName     string
		channel    string
		sendErr    error
		wantStatus string
	}{
		{"success", "dispatch_outcomes_success", inbox.ChannelLiveChat, nil, models.MessageStatusSent},
		{"livechat offline is sent from history", "dispatch_outcomes_offline", inbox.ChannelLiveChat, livechat.ErrClientNotConnected, models.MessageStatusSent},
		{"definitive rejection", "dispatch_outcomes_rejected", inbox.ChannelWhatsApp, errors.New("whatsapp rejected the message: bad request"), models.MessageStatusFailed},
		{"uncertain channel result", "dispatch_outcomes_uncertain", inbox.ChannelWhatsApp, whatsappChannel.ErrDeliveryUncertain, models.MessageStatusUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testutil.NewDB(t, tc.dbName)
			contactID, senderID, inboxID := seedDispatchIdentities(t, db, tc.channel)
			convID := seedDispatchConversation(t, db, contactID, inboxID)

			inb := &dispatchFakeInbox{id: inboxID, channel: tc.channel, sendErr: tc.sendErr}
			m, wh := newDispatchManager(t, db, inb)

			id := insertDispatchMessage(t, db, convID, senderID)
			m.sendOutgoingMessage(models.Message{ID: id, ConversationID: convID})

			if inb.calls != 1 {
				t.Fatalf("channel must be called once, got %d", inb.calls)
			}
			if inb.lastGen != 1 {
				t.Fatalf("channel call must carry generation 1, got %d", inb.lastGen)
			}
			status, _, _, owner := messageStatus(t, db, id)
			if status != tc.wantStatus {
				t.Fatalf("want status %s, got %s", tc.wantStatus, status)
			}
			if owner != nil {
				t.Fatalf("terminal row must release its owner")
			}
			if wh.updated != 1 {
				t.Fatalf("message_updated must fire once, got %d", wh.updated)
			}

			// Running the same peeked message again can never call the
			// channel twice: only a pending row can be claimed.
			m.sendOutgoingMessage(models.Message{ID: id, ConversationID: convID})
			if inb.calls != 1 {
				t.Fatalf("a finalized message must never be dispatched again, calls=%d", inb.calls)
			}
			if wh.updated != 1 {
				t.Fatalf("side effects must not repeat, webhooks=%d", wh.updated)
			}
		})
	}
}

// Two workers racing for the same peeked message result in one channel call.
func TestDuplicateWorkersSendOnce(t *testing.T) {
	db := testutil.NewDB(t, "dispatch_race")
	contactID, senderID, inboxID := seedDispatchIdentities(t, db, inbox.ChannelLiveChat)
	convID := seedDispatchConversation(t, db, contactID, inboxID)
	inb := &dispatchFakeInbox{id: inboxID, channel: inbox.ChannelLiveChat}
	m, _ := newDispatchManager(t, db, inb)

	id := insertDispatchMessage(t, db, convID, senderID)
	done := make(chan struct{}, 2)
	go func() { m.sendOutgoingMessage(models.Message{ID: id}); done <- struct{}{} }()
	m.sendOutgoingMessage(models.Message{ID: id})
	<-done

	if inb.calls != 1 {
		t.Fatalf("expected exactly one channel send, got %d", inb.calls)
	}
}

// A receipt that converges an unknown parked row drives the one-shot outgoing
// side effects (message_updated webhook, reply timestamp) exactly once.
func TestUnknownConvergesByReceiptWithSideEffectsOnce(t *testing.T) {
	db := testutil.NewDB(t, "dispatch_converge")
	contactID, senderID, inboxID := seedDispatchIdentities(t, db, inbox.ChannelWhatsApp)
	convID := seedDispatchConversation(t, db, contactID, inboxID)
	inb := &dispatchFakeInbox{id: inboxID, channel: inbox.ChannelWhatsApp,
		sendErr: whatsappChannel.ErrDeliveryUncertain}
	m, wh := newDispatchManager(t, db, inb)

	id := insertDispatchMessage(t, db, convID, senderID)
	messageUUID := uuidForID(t, db, id)

	// Channel uncertain: parked as unknown without advancing reply time.
	m.sendOutgoingMessage(models.Message{ID: id})
	status, _, _, _ := messageStatus(t, db, id)
	if status != models.MessageStatusUnknown {
		t.Fatalf("want unknown, got %s", status)
	}
	var lastReply *time.Time
	if err := db.QueryRow(`SELECT last_reply_at FROM conversations WHERE id = $1`, convID).Scan(&lastReply); err != nil {
		t.Fatal(err)
	}
	if lastReply != nil {
		t.Fatalf("unknown must not advance reply timestamps")
	}

	// The zombie owner reports its wamid late: it is still generation 1, so it
	// is stored to let receipts match.
	if err := m.UpdateMessageSourceID(messageUUID, "wamid.RECOVERED", 1); err != nil {
		t.Fatal(err)
	}
	// Meta confirms delivery: the row converges and side effects fire once.
	if err := m.ApplyWhatsAppStatus("wamid.RECOVERED", WhatsAppStatusDelivered, time.Now(), ""); err != nil {
		t.Fatal(err)
	}
	status, gen, _, _ := messageStatus(t, db, id)
	if status != models.MessageStatusSent || gen != 1 {
		t.Fatalf("want gen 1 sent, got %s gen %d", status, gen)
	}
	if err := db.QueryRow(`SELECT last_reply_at FROM conversations WHERE id = $1`, convID).Scan(&lastReply); err != nil {
		t.Fatal(err)
	}
	if lastReply == nil {
		t.Fatalf("convergence must advance the reply timestamp once")
	}
	updatedAfterFirst := wh.updated

	// A duplicate read receipt changes neither the effects nor the status.
	if err := m.ApplyWhatsAppStatus("wamid.RECOVERED", WhatsAppStatusRead, time.Now(), ""); err != nil {
		t.Fatal(err)
	}
	if wh.updated != updatedAfterFirst {
		t.Fatalf("ordinary lifecycle receipts must not repeat terminal side effects")
	}

	// Manual retry after convergence is still possible, on a new generation.
	if err := m.MarkMessageAsPending(messageUUID); err != nil {
		t.Fatal(err)
	}
	status, gen, sourceID, _ := messageStatus(t, db, id)
	if status != models.MessageStatusPending || gen != 2 || sourceID != nil {
		t.Fatalf("retry must reset to pending gen 2 without the old wamid, got %s %d %v", status, gen, sourceID)
	}
}

// ---------------------------------------------------------------------------
// Seed helpers.
// ---------------------------------------------------------------------------

func seedDispatchIdentities(t *testing.T, db *sqlx.DB, channel string) (contactID, senderID, inboxID int) {
	t.Helper()
	if err := db.QueryRow(`INSERT INTO users (type, email, first_name, last_name) VALUES ('contact', gen_random_uuid()::text||'@example.test', 'C', '') RETURNING id`).Scan(&contactID); err != nil {
		t.Fatalf("insert contact: %v", err)
	}
	if err := db.QueryRow(`INSERT INTO users (type, email, first_name, last_name) VALUES ('agent', gen_random_uuid()::text||'@example.test', 'Agent', '') RETURNING id`).Scan(&senderID); err != nil {
		t.Fatalf("insert agent: %v", err)
	}
	// afterOutgoingSent distinguishes human replies from the seeded system user.
	if _, err := db.Exec(`INSERT INTO users (type, email, first_name, last_name) VALUES ('agent', 'System', 'System', '')`); err != nil {
		t.Fatalf("insert system user: %v", err)
	}
	if err := db.QueryRow(`INSERT INTO inboxes (channel, name, "from") VALUES ($1, 'dispatch', '') RETURNING id`, channel).Scan(&inboxID); err != nil {
		t.Fatalf("insert inbox: %v", err)
	}
	return
}

func seedDispatchConversation(t *testing.T, db *sqlx.DB, contactID, inboxID int) int {
	t.Helper()
	var id int
	if err := db.QueryRow(`
		INSERT INTO conversations (contact_id, inbox_id, status_id, subject)
		VALUES ($1, $2, (SELECT id FROM conversation_statuses WHERE category = 'open' LIMIT 1), 'dispatch')
		RETURNING id`, contactID, inboxID).Scan(&id); err != nil {
		t.Fatalf("insert conversation: %v", err)
	}
	return id
}

func uuidForID(t *testing.T, db *sqlx.DB, id int) string {
	t.Helper()
	var u string
	if err := db.QueryRow(`SELECT uuid::text FROM conversation_messages WHERE id = $1`, id).Scan(&u); err != nil {
		t.Fatalf("fetching uuid: %v", err)
	}
	return u
}


