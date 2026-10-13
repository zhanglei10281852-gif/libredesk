package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abhinavxd/libredesk/internal/automation"
	"github.com/abhinavxd/libredesk/internal/conversation"
	mmodels "github.com/abhinavxd/libredesk/internal/media/models"
	"github.com/abhinavxd/libredesk/internal/user"
	"github.com/abhinavxd/libredesk/internal/whatsapp"
	"github.com/abhinavxd/libredesk/internal/whatsapp/eventlog"
	wt "github.com/abhinavxd/libredesk/internal/whatsapp/template"
	wmodels "github.com/abhinavxd/libredesk/internal/webhook/models"
	"github.com/abhinavxd/libredesk/internal/ws"
	"github.com/jmoiron/sqlx"
	"github.com/volatiletech/null/v9"
)

// waNoopMedia satisfies the conversation media store without touching disk.
type waNoopMedia struct{}

func (waNoopMedia) Get(int, string) (mmodels.Media, error) { return mmodels.Media{}, nil }
func (waNoopMedia) GetBlob(string) ([]byte, error)        { return nil, nil }
func (waNoopMedia) GetURL(string, string, string) string  { return "" }
func (waNoopMedia) GetSignedURL(string) string            { return "" }
func (waNoopMedia) GetThumbnailURL(string) string         { return "" }
func (waNoopMedia) LinkMessageMediaTx(*sqlx.Tx, int, []mmodels.Media, []string, int) error {
	return nil
}
func (waNoopMedia) GetByModel(int, string) ([]mmodels.Media, error) { return nil, nil }
func (waNoopMedia) GetByContentIDs([]string, string) ([]mmodels.Media, error) {
	return nil, nil
}
func (waNoopMedia) GetDraftInlineMedia(string, int, int) (mmodels.Media, error) {
	return mmodels.Media{}, nil
}
func (waNoopMedia) ContentIDExists(string, string) (bool, string, error) {
	return false, "", nil
}
func (waNoopMedia) Upload(string, string, io.ReadSeeker) (string, string, error) {
	return "", "", nil
}
func (waNoopMedia) UploadAndInsert(string, string, string, null.String, null.Int, io.ReadSeeker, int, null.String, []byte, bool) (mmodels.Media, error) {
	return mmodels.Media{}, nil
}

// waNoopWebhooks absorbs webhook triggers in tests.
type waNoopWebhooks struct{}

func (waNoopWebhooks) TriggerEvent(wmodels.WebhookEvent, any) {}
func (waNoopWebhooks) TriggerWebhook(int, wmodels.WebhookEvent, any) {}

func mustParsePayload(t *testing.T, body string) *whatsapp.WebhookPayload {
	t.Helper()
	p, err := whatsapp.ParsePayload([]byte(body))
	if err != nil {
		t.Fatalf("parsing payload: %v", err)
	}
	return p
}

func eventKeys(events []waBusinessEvent) []string {
	var out []string
	for _, ev := range events {
		out = append(out, ev.keyText)
	}
	return out
}

// Splitting must produce identity-stable, order-stable units: messages, then statuses, then templates.
func TestSplitWhatsAppBusinessEventsStable(t *testing.T) {
	app, _ := testIngesterApp(t)

	body := fmt.Sprintf(`{
	  "object": "whatsapp_business_account",
	  "entry": [{
	    "id": "WABA-1",
	    "changes": [
	      {"field": "messages", "value": {
	        "metadata": {"phone_number_id": "PN-A"},
	        "contacts": [{"profile": {"name": "Ravi"}, "wa_id": "15550000100"}],
	        "messages": [
	          {"from": "15550000100", "id": "wamid.m1", "timestamp": "1700000000", "type": "text", "text": {"body": "one"}},
	          {"from": "15550000100", "id": "wamid.m2", "timestamp": "1700000010", "type": "text", "text": {"body": "two"}}
	        ],
	        "statuses": [{"id": "wamid.s1", "status": "delivered", "timestamp": "1700000020"}]
	      }},
	      {"field": "messages", "value": {
	        "metadata": {},
	        "messages": [{"from": "", "id": "", "timestamp": "1700000030", "type": "text", "text": {"body": "bad"}}]
	      }}
	    ]
	  }, {
	    "id": "",
	    "changes": [{"field": "message_template_status_update", "value": {
	      "event": "APPROVED", "message_template_name": "order_confirm", "message_template_language": "en",
	      "message_template_id": "123", "reason": "NONE"
	    }}]
	  }]
	}`)
	payload := mustParsePayload(t, body)

	first, err := splitWhatsAppBusinessEvents(app, 7, payload)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	second, err := splitWhatsAppBusinessEvents(app, 7, payload)
	if err != nil {
		t.Fatalf("split (2): %v", err)
	}

	got := eventKeys(first)
	want := []string{
		"wa:msg:wamid.m1",
		"wa:msg:wamid.m2",
		"wa:msg:bad:e0.c1.i0:", // prefix only, digest asserted separately
		"wa:st:wamid.s1:delivered",
		"wa:tpl:-:123:en:APPROVED:inbox:7",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d units, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if !strings.HasPrefix(got[i], want[i]) {
			t.Fatalf("unit %d: key %q does not start with %q", i, got[i], want[i])
		}
	}
	if !strings.Contains(got[2], ":bad:") || len(strings.TrimPrefix(got[2], "wa:msg:bad:e0.c1.i0:")) != 12 {
		t.Fatalf("bad-message key must carry a 12-char digest, got %q", got[2])
	}
	if fmt.Sprint(eventKeys(second)) != fmt.Sprint(got) {
		t.Fatalf("split is not deterministic:\n%v\n%v", got, eventKeys(second))
	}
	if first[0].kind != eventlog.KindMessage || first[3].kind != eventlog.KindStatus || first[4].kind != eventlog.KindTemplate {
		t.Fatalf("unexpected kinds: %q %q %q", first[0].kind, first[3].kind, first[4].kind)
	}

	// A status receipt missing its identity gets a synthetic permanent key as well.
	badStatus := mustParsePayload(t, `{"entry":[{"changes":[{"field":"messages","value":{
	  "statuses":[{"id":"","status":"delivered","timestamp":"1700000000"}]}}]}]}`)
	units, err := splitWhatsAppBusinessEvents(app, 7, badStatus)
	if err != nil || len(units) != 1 || !strings.HasPrefix(units[0].keyText, "wa:st:bad:") {
		t.Fatalf("expected one bad-status unit, got %+v err=%v", units, err)
	}
}

// A template status fans out to every inbox of the WABA as independent units, sorted for stable order.
func TestSplitTemplateFanOutPerInbox(t *testing.T) {
	app, db := newInboxApp(t)

	var idA, idB int
	if err := db.Get(&idA, `INSERT INTO inboxes (channel, config, "name", enabled, "from")
		VALUES ('whatsapp', '{"phone_number_id":"PN-A","waba_id":"WABA-X"}'::jsonb, 'wa-a', true, '') RETURNING id`); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(&idB, `INSERT INTO inboxes (channel, config, "name", enabled, "from")
		VALUES ('whatsapp', '{"phone_number_id":"PN-B","waba_id":"WABA-X"}'::jsonb, 'wa-b', true, '') RETURNING id`); err != nil {
		t.Fatal(err)
	}
	tmgr, err := wt.New(wt.Opts{Lo: app.lo, DB: db, I18n: app.i18n})
	if err != nil {
		t.Fatal(err)
	}
	app.whatsappTemplate = tmgr

	payload := mustParsePayload(t, fmt.Sprintf(`{
	  "entry": [
	    {"id": "WABA-X", "changes": [{"field": "message_template_status_update", "value": {
	      "event": "APPROVED", "message_template_id": "123", "message_template_name": "t", "message_template_language": "en"}}]},
	    {"id": "WABA-NOPE", "changes": [{"field": "message_template_status_update", "value": {
	      "event": "REJECTED", "message_template_id": "123", "message_template_name": "t", "message_template_language": "en", "reason": "bad"}}]},
	    {"id": "", "changes": [{"field": "message_template_status_update", "value": {
	      "event": "PAUSED", "message_template_id": "123", "message_template_name": "t", "message_template_language": "en"}}]}
	  ]
	}`))

	units, err := splitWhatsAppBusinessEvents(app, 9, payload)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	got := eventKeys(units)
	lo, hi := min(idA, idB), max(idA, idB)
	want := []string{
		fmt.Sprintf("wa:tpl:WABA-X:123:en:APPROVED:inbox:%d", lo),
		fmt.Sprintf("wa:tpl:WABA-X:123:en:APPROVED:inbox:%d", hi),
		"wa:tpl:WABA-NOPE:123:en:REJECTED:none",
		"wa:tpl:-:123:en:PAUSED:inbox:9",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("fan-out units:\n got %v\nwant %v", got, want)
	}

	// The unmatched-WABA target is pre-routed to inbox 0 and must classify as terminal-ignored.
	var zeroTarget waTemplateStatusRecord
	if err := json.Unmarshal(mustJSON(units[2].record), &zeroTarget); err != nil {
		t.Fatal(err)
	}
	if zeroTarget.TargetInboxID != 0 {
		t.Fatalf("expected no-target unit to carry inbox 0, got %d", zeroTarget.TargetInboxID)
	}
	if out := executeWhatsAppTemplateEvent(t.Context(), app, &eventlog.Event{Payload: mustJSON(units[2].record)}); out.state != eventlog.StatusIgnored {
		t.Fatalf("expected ignored, got %+v", out)
	}
}

// Claim/terminal/defer/stale-lease semantics of the progress store.
func TestEventStoreProgressLifecycle(t *testing.T) {
	app, db := newInboxApp(t)
	store := app.whatsappEvents
	ctx := t.Context()
	payload := json.RawMessage(`{}`)

	e, err := store.Ensure(ctx, 7, eventlog.KindMessage, "wa:msg:life", payload)
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != eventlog.StatusPending {
		t.Fatalf("new unit should be pending, got %s", e.Status)
	}
	if again, _ := store.Ensure(ctx, 7, eventlog.KindMessage, "wa:msg:life", payload); again.ID != e.ID {
		t.Fatal("redelivery must map to the same row")
	}

	claimed, ok, err := store.Claim(ctx, e.ID, "owner-a", time.Minute)
	if err != nil || !ok || claimed.Attempts != 1 {
		t.Fatalf("first claim: ok=%v attempts=%d err=%v", ok, claimedAttempts(claimed), err)
	}
	if _, ok, _ := store.Claim(ctx, e.ID, "owner-b", time.Minute); ok {
		t.Fatal("a live lease must not be claimable by a second worker")
	}

	if err := store.MarkRetry(ctx, e.ID, "media download failed", "owner-a"); err != nil {
		t.Fatal(err)
	}
	row, err := store.Get(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != eventlog.StatusPending || !row.LastError.Valid {
		t.Fatalf("expected pending with error, got %s %+v", row.Status, row.LastError)
	}

	if _, ok, _ = store.Claim(ctx, e.ID, "owner-a", time.Minute); !ok {
		t.Fatal("retry claim failed")
	}
	if err := store.MarkDeferred(ctx, e.ID, time.Now().Add(time.Hour), "waiting for message row", "owner-a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ = store.Claim(ctx, e.ID, "owner-a", time.Minute); ok {
		t.Fatal("a deferred unit before not_before must not be claimed")
	}
	if _, err := db.Exec(`UPDATE whatsapp_inbound_events SET not_before = NOW() - INTERVAL '1 minute' WHERE id = $1`, e.ID); err != nil {
		t.Fatal(err)
	}
	claimed, ok, _ = store.Claim(ctx, e.ID, "owner-a", time.Minute)
	if !ok || claimed.Attempts != 3 {
		t.Fatalf("deferred claim after not_before: ok=%v attempts=%d", ok, claimedAttempts(claimed))
	}
	if err := store.MarkSucceeded(ctx, e.ID, "owner-a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := store.Claim(ctx, e.ID, "owner-a", time.Minute); ok {
		t.Fatal("a terminal unit must never be claimed")
	}

	// Stale lease takeover: the dead owner's transition is rejected, the new owner decides the outcome.
	e2, _ := store.Ensure(ctx, 7, eventlog.KindStatus, "wa:st:x:read", payload)
	if _, ok, _ := store.Claim(ctx, e2.ID, "dead-owner", time.Minute); !ok {
		t.Fatal("claim")
	}
	if _, err := db.Exec(`UPDATE whatsapp_inbound_events SET lease_at = NOW() - INTERVAL '15 minutes' WHERE id = $1`, e2.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := store.Claim(ctx, e2.ID, "new-owner", 10*time.Minute); !ok {
		t.Fatal("a stale lease must be takeover-able after the reclaim idle")
	}
	if err := store.MarkSucceeded(ctx, e2.ID, "dead-owner"); err != eventlog.ErrLeaseLost {
		t.Fatalf("dead owner transition must fail with ErrLeaseLost, got %v", err)
	}
	if err := store.MarkPermanent(ctx, e2.ID, "unparseable identity", "new-owner"); err != nil {
		t.Fatal(err)
	}
	final, _ := store.Get(ctx, e2.ID)
	if final.Status != eventlog.StatusPermanent || final.LastError.String != "unparseable identity" {
		t.Fatalf("permanent outcome not stored: %+v", final)
	}

	// Pruning only removes aged terminal rows.
	if _, err := db.Exec(`UPDATE whatsapp_inbound_events SET processed_at = NOW() - INTERVAL '60 days' WHERE id = $1`, e.ID); err != nil {
		t.Fatal(err)
	}
	n, err := store.Prune(ctx, 30*24*time.Hour, 1000*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected one pruned succeeded row, got %d", n)
	}
	if left, _ := store.Get(ctx, e2.ID); left == nil || left.Status != eventlog.StatusPermanent {
		t.Fatal("permanent rows must be retained for diagnostics")
	}
}

func claimedAttempts(e *eventlog.Event) int {
	if e == nil {
		return -1
	}
	return e.Attempts
}

// End-to-end: successes stay done across redeliveries, only unfinished units re-run, and the
// delivery acks once everything is terminal.
func TestProcessPayloadEventProgressEndToEnd(t *testing.T) {
	app, db := newWhatsAppConversationApp(t)

	var inboxID int
	if err := db.Get(&inboxID, `INSERT INTO inboxes (channel, name, "from", config, enabled, reopen_window_hours)
		VALUES ('whatsapp', 'E2E', '', '{"phone_number_id":"PN-E2E"}', true, 24) RETURNING id`); err != nil {
		t.Fatal(err)
	}

	now := time.Now().Unix()
	old := time.Now().Add(-2 * whatsAppStatusNotFoundGrace).Unix()
	body := fmt.Sprintf(`{
	  "object": "whatsapp_business_account",
	  "entry": [{
	    "id": "WABA-1",
	    "changes": [
	      {"field": "messages", "value": {
	        "metadata": {"phone_number_id": "PN-E2E"},
	        "contacts": [{"profile": {"name": "Ravi"}, "wa_id": "15550009999"}],
	        "messages": [
	          {"from": "15550009999", "id": "wamid.e2e.m1", "timestamp": "%d", "type": "text", "text": {"body": "one"}},
	          {"from": "15550009999", "id": "wamid.e2e.m2", "timestamp": "%d", "type": "text", "text": {"body": "two"}},
	          {"from": "", "id": "", "timestamp": "%d", "type": "text", "text": {"body": "bad"}}
	        ],
	        "statuses": [
	          {"id": "wamid.e2e.out-late", "status": "delivered", "timestamp": "%d"},
	          {"id": "wamid.e2e.out-old", "status": "read", "timestamp": "%d"}
	        ]
	      }},
	      {"field": "message_template_status_update", "value": {
	        "event": "APPROVED", "message_template_name": "t", "message_template_language": "en"}}
	    ]
	  }]
	}`, now-60, now-50, now-40, now-30, old)
	payload := mustParsePayload(t, body)

	// Run 1: two messages ingest, bad unit is permanent, late status defers, old status and the
	// no-manager template unit ignore; the delivery stays pending.
	if err := processWhatsAppPayload(t.Context(), app, inboxID, payload); err == nil {
		t.Fatal("the deferred status must keep the delivery pending")
	}
	assertMessageCount(t, db, 2)
	counts := eventStatusCounts(t, db)
	wantCounts := map[eventlog.Status]int{
		eventlog.StatusSucceeded: 2,
		eventlog.StatusPermanent: 1,
		eventlog.StatusDeferred:  1,
		eventlog.StatusIgnored:   2,
	}
	assertStatusCounts(t, counts, wantCounts)

	msgAttempts := eventAttempts(t, db, "wa:msg:wamid.e2e.m1")
	if msgAttempts != 1 {
		t.Fatalf("expected 1 claim for the message, got %d", msgAttempts)
	}

	// Run 2 (Meta redelivery / reclaim while the outbound row is still missing): terminal units
	// must not re-execute and no duplicate messages may appear.
	if err := processWhatsAppPayload(t.Context(), app, inboxID, payload); err == nil {
		t.Fatal("delivery must still wait for the deferred status")
	}
	assertMessageCount(t, db, 2)
	if got := eventAttempts(t, db, "wa:msg:wamid.e2e.m1"); got != 1 {
		t.Fatalf("succeeded message was re-claimed, attempts=%d", got)
	}
	if got := eventAttempts(t, db, "wa:msg:wamid.e2e.m2"); got != 1 {
		t.Fatalf("succeeded message was re-claimed, attempts=%d", got)
	}

	// The outbound row lands late. Park the deferred row's not_before in the past to emulate the
	// 10-minute reclaim boundary, then run 3: the receipt applies and the delivery acks.
	seedOutboundMessage(t, db, inboxID, "15550009999", "wamid.e2e.out-late")
	if _, err := db.Exec(`UPDATE whatsapp_inbound_events SET not_before = NOW() - INTERVAL '1 minute'
		WHERE event_key = 'wa:st:wamid.e2e.out-late:delivered'`); err != nil {
		t.Fatal(err)
	}
	if err := processWhatsAppPayload(t.Context(), app, inboxID, payload); err != nil {
		t.Fatalf("all units terminal, delivery should ack: %v", err)
	}
	assertMessageCount(t, db, 2)
	var providerStatus string
	if err := db.Get(&providerStatus, `SELECT meta->>'provider_status' FROM conversation_messages WHERE source_id = 'wamid.e2e.out-late'`); err != nil {
		t.Fatal(err)
	}
	if providerStatus != "delivered" {
		t.Fatalf("deferred receipt not applied after the message arrived: %q", providerStatus)
	}
	counts = eventStatusCounts(t, db)
	wantCounts[eventlog.StatusDeferred] = 0
	wantCounts[eventlog.StatusSucceeded] = 3
	assertStatusCounts(t, counts, wantCounts)

	// Run 4: fully terminal delivery re-delivered byte-identically must be a no-op ack.
	if err := processWhatsAppPayload(t.Context(), app, inboxID, payload); err != nil {
		t.Fatalf("terminal redelivery: %v", err)
	}
	assertMessageCount(t, db, 2)
}

// A transient media failure parks just that message unit; a later run completes it.
func TestTransientMediaFailureRetriesOnlyItsUnit(t *testing.T) {
	app, db := newWhatsAppConversationApp(t)

	var inboxID int
	if err := db.Get(&inboxID, `INSERT INTO inboxes (channel, name, "from", config, enabled, reopen_window_hours)
		VALUES ('whatsapp', 'Media', '', '{"phone_number_id":"PN-MEDIA"}', true, 24) RETURNING id`); err != nil {
		t.Fatal(err)
	}

	var failures atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// First three lookup attempts fail transiently, then the media is permanently gone
		// (404) and ingestion completes with a placeholder.
		if failures.Add(1) <= 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	app.whatsappClient = whatsapp.New(app.lo)
	app.whatsappClient.SetBaseURL(server.URL)

	body := fmt.Sprintf(`{
	  "entry": [{"changes": [{"field": "messages", "value": {
	    "metadata": {"phone_number_id": "PN-MEDIA"},
	    "contacts": [{"profile": {"name": "Ravi"}, "wa_id": "15550008888"}],
	    "messages": [{"from": "15550008888", "id": "wamid.media.1", "timestamp": "%d", "type": "image",
	      "image": {"id": "media-1", "mime_type": "image/jpeg"}}]
	  }}]}]
	}`, time.Now().Unix())
	payload := mustParsePayload(t, body)

	if err := processWhatsAppPayload(t.Context(), app, inboxID, payload); err == nil {
		t.Fatal("the transient media failure must park the delivery")
	}
	assertMessageCount(t, db, 0)
	if got := eventStatusCounts(t, db)[eventlog.StatusPending]; got != 1 {
		t.Fatalf("expected one pending media unit, got %d", got)
	}

	// Fourth attempt downloads successfully; the message is stored exactly once.
	if err := processWhatsAppPayload(t.Context(), app, inboxID, payload); err != nil {
		t.Fatalf("media recovered, delivery should ack: %v", err)
	}
	assertMessageCount(t, db, 1)
	if got := eventStatusCounts(t, db)[eventlog.StatusSucceeded]; got != 1 {
		t.Fatal("media unit must be succeeded")
	}
	if err := processWhatsAppPayload(t.Context(), app, inboxID, payload); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	assertMessageCount(t, db, 1)
}

func assertMessageCount(t *testing.T, db *sqlx.DB, want int) {
	t.Helper()
	var got int
	if err := db.Get(&got, `SELECT COUNT(*) FROM conversation_messages WHERE type = 'incoming'`); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("expected %d incoming messages, got %d", want, got)
	}
}

func eventStatusCounts(t *testing.T, db *sqlx.DB) map[eventlog.Status]int {
	t.Helper()
	rows, err := db.Queryx(`SELECT status, COUNT(*) FROM whatsapp_inbound_events GROUP BY status`)
	if err != nil {
		t.Fatal(err)
	}
	out := map[eventlog.Status]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			t.Fatal(err)
		}
		out[eventlog.Status(st)] = n
	}
	return out
}

func assertStatusCounts(t *testing.T, got, want map[eventlog.Status]int) {
	t.Helper()
	for st, n := range want {
		if got[st] != n {
			t.Fatalf("status %s: want %d rows, got %d (all: %v)", st, n, got[st], got)
		}
	}
	for st, n := range got {
		if want[st] != n {
			t.Fatalf("unexpected rows in status %s: %d (all: %v)", st, n, got)
		}
	}
}

func eventAttempts(t *testing.T, db *sqlx.DB, key string) int {
	t.Helper()
	var n int
	if err := db.Get(&n, `SELECT attempts FROM whatsapp_inbound_events WHERE event_key = $1`, key); err != nil {
		t.Fatal(err)
	}
	return n
}

func seedOutboundMessage(t *testing.T, db *sqlx.DB, inboxID int, waID, sourceID string) {
	t.Helper()
	var contactID int
	if err := db.Get(&contactID, `INSERT INTO users (type, first_name, last_name, enabled)
		VALUES ('contact', 'Customer', '', true) RETURNING id`); err != nil {
		t.Fatal(err)
	}
	_ = waID
	var conversationID int
	if err := db.Get(&conversationID, `INSERT INTO conversations (contact_id, inbox_id, status_id, last_inbound_at)
		VALUES ($1, $2, (SELECT id FROM conversation_statuses WHERE category = 'open' LIMIT 1), NOW())
		RETURNING id`, contactID, inboxID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO conversation_messages
		(conversation_id, sender_id, sender_type, type, status, content, text_content, content_type, source_id, meta)
		VALUES ($1, $2, 'agent', 'outgoing', 'sent', 'hello', 'hello', 'text', $3, '{}'::jsonb)`,
		conversationID, contactID, sourceID); err != nil {
		t.Fatal(err)
	}
}

// newWhatsAppConversationApp wires user + conversation managers against a fresh DB.
func newWhatsAppConversationApp(t *testing.T) (*App, *sqlx.DB) {
	t.Helper()
	app, db := newInboxApp(t)
	var err error
	app.user, err = user.New(app.i18n, user.Opts{DB: db, Lo: app.lo})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := automation.New(automation.Opts{DB: db, Lo: app.lo, I18n: app.i18n})
	if err != nil {
		t.Fatal(err)
	}
	app.conversation, err = conversation.New(
		ws.NewHub(app.lo, nil /** userStore **/),
		app.i18n,
		nil, /** sla **/ nil, /** status **/ nil, /** priority **/
		app.inbox, app.user,
		nil,             /** team **/
		waNoopMedia{},   /** media **/
		nil,             /** settings **/
		nil,             /** csat **/
		engine,          /** automation **/
		nil,             /** template **/
		waNoopWebhooks{}, /** webhook **/
		nil,             /** dispatcher **/
		conversation.Opts{DB: db, Lo: app.lo},
	)
	if err != nil {
		t.Fatal(err)
	}
	return app, db
}
