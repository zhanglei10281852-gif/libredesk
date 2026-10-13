package main

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/abhinavxd/libredesk/internal/conversation"
	"github.com/abhinavxd/libredesk/internal/whatsapp"
	"github.com/abhinavxd/libredesk/internal/whatsapp/eventlog"
	wtmodels "github.com/abhinavxd/libredesk/internal/whatsapp/template/models"
)

const (
	// waEventKeyPrefix namespaces every stable business-event key.
	waEventKeyPrefix = "wa"

	// Terminal rows past these ages are pruned. Permanent failures are kept longer for diagnostics.
	waEventTerminalRetention  = 30 * 24 * time.Hour
	waEventPermanentRetention = 90 * 24 * time.Hour
	waEventPruneInterval      = time.Hour
)

// Persisted payloads: the event can be re-executed after a restart without keeping the raw delivery body.

type waInboundMessageRecord struct {
	URLInboxID int                    `json:"url_inbox_id"`
	Message    whatsapp.ParsedMessage `json:"message"`
}

type waInboundStatusRecord struct {
	Status whatsapp.ParsedStatus `json:"status"`
}

type waTemplateStatusRecord struct {
	URLInboxID    int                           `json:"url_inbox_id"`
	TargetInboxID int                           `json:"target_inbox_id"`
	Update        whatsapp.ParsedTemplateStatus `json:"update"`
}

// waBusinessEvent is one unit of work split from a delivery: one message, one status
// receipt, or one template-status fan-out target.
type waBusinessEvent struct {
	keyText string
	kind    eventlog.Kind
	inboxID int
	record  any
}

// splitWhatsAppBusinessEvents breaks a parsed delivery into stable-identity, stable-order
// business units. A transient fan-out lookup error fails the split so the delivery retries;
// already-terminal units are skipped on the next run.
func splitWhatsAppBusinessEvents(app *App, urlInboxID int, p *whatsapp.WebhookPayload) ([]waBusinessEvent, error) {
	enumerated := p.EnumerateEvents()

	hasTemplateUpdates := false
	for _, ev := range enumerated {
		if ev.Kind == whatsapp.EventKindTemplate {
			hasTemplateUpdates = true
			break
		}
	}

	// WABA -> enabled inbox ids, resolved once per delivery for the template fan-out.
	var wabaInboxes map[string][]int
	if hasTemplateUpdates && app.whatsappTemplate != nil {
		m, err := whatsAppInboxIDsByWABA(app)
		if err != nil {
			return nil, fmt.Errorf("listing inboxes for template status updates: %w", err)
		}
		wabaInboxes = m
	}

	events := make([]waBusinessEvent, 0, len(enumerated))
	for _, ev := range enumerated {
		switch ev.Kind {
		case whatsapp.EventKindMessage:
			events = append(events, whatsAppMessageEvent(urlInboxID, ev))
		case whatsapp.EventKindStatus:
			events = append(events, whatsAppStatusEvent(urlInboxID, ev))
		case whatsapp.EventKindTemplate:
			events = append(events, whatsAppTemplateEvents(urlInboxID, wabaInboxes, ev)...)
		}
	}
	return events, nil
}

func whatsAppMessageEvent(urlInboxID int, ev whatsapp.PayloadEvent) waBusinessEvent {
	rec := waInboundMessageRecord{URLInboxID: urlInboxID, Message: ev.Message}
	if ev.Message.ID == "" || ev.Message.From == "" {
		return waBusinessEvent{
			keyText: waBadEventKey("msg", ev, rec),
			kind:    eventlog.KindMessage,
			inboxID: urlInboxID,
			record:  rec,
		}
	}
	return waBusinessEvent{
		keyText: waEventKeyPrefix + ":msg:" + ev.Message.ID,
		kind:    eventlog.KindMessage,
		inboxID: urlInboxID,
		record:  rec,
	}
}

func whatsAppStatusEvent(urlInboxID int, ev whatsapp.PayloadEvent) waBusinessEvent {
	rec := waInboundStatusRecord{Status: ev.Status}
	if ev.Status.MessageID == "" || ev.Status.Status == "" {
		return waBusinessEvent{
			keyText: waBadEventKey("st", ev, rec),
			kind:    eventlog.KindStatus,
			inboxID: urlInboxID,
			record:  rec,
		}
	}
	return waBusinessEvent{
		keyText: waEventKeyPrefix + ":st:" + ev.Status.MessageID + ":" + ev.Status.Status,
		kind:    eventlog.KindStatus,
		inboxID: urlInboxID,
		record:  rec,
	}
}

// whatsAppTemplateEvents fans one template status update out to every enabled inbox of the
// WABA (or the URL inbox when the WABA id is absent). Each target is its own unit so a
// single failing target retries independently.
func whatsAppTemplateEvents(urlInboxID int, wabaInboxes map[string][]int, ev whatsapp.PayloadEvent) []waBusinessEvent {
	u := ev.Template
	wabaSegment := u.WABAID
	if wabaSegment == "" {
		wabaSegment = "-"
	}

	identity := u.MetaTemplateID
	if identity == "" {
		identity = "name:" + u.TemplateName
	}
	baseKey := fmt.Sprintf("%s:tpl:%s:%s:%s:%s", waEventKeyPrefix, wabaSegment, identity, u.Language, u.Event)

	// Target 0 marks the "no routable inbox" outcome, which is terminal-ignored at execution.
	if u.MetaTemplateID == "" && u.TemplateName == "" {
		rec := waTemplateStatusRecord{URLInboxID: urlInboxID, TargetInboxID: 0, Update: u}
		return []waBusinessEvent{{
			keyText: waBadEventKey("tpl", ev, rec),
			kind:    eventlog.KindTemplate,
			inboxID: urlInboxID,
			record:  rec,
		}}
	}

	var ids []int
	if u.WABAID == "" {
		ids = []int{urlInboxID}
	} else {
		ids = append(ids, wabaInboxes[u.WABAID]...)
		sort.Ints(ids)
	}

	if len(ids) == 0 {
		rec := waTemplateStatusRecord{URLInboxID: urlInboxID, TargetInboxID: 0, Update: u}
		return []waBusinessEvent{{
			keyText: baseKey + ":none",
			kind:    eventlog.KindTemplate,
			inboxID: urlInboxID,
			record:  rec,
		}}
	}

	out := make([]waBusinessEvent, 0, len(ids))
	for _, id := range ids {
		out = append(out, waBusinessEvent{
			keyText: fmt.Sprintf("%s:inbox:%d", baseKey, id),
			kind:    eventlog.KindTemplate,
			inboxID: urlInboxID,
			record: waTemplateStatusRecord{
				URLInboxID:    urlInboxID,
				TargetInboxID: id,
				Update:        u,
			},
		})
	}
	return out
}

// waBadEventKey builds a stable key for an identity-less item from its payload coordinates
// and a digest of its content, so identical redeliveries hit the same permanent record.
func waBadEventKey(kindAbbr string, ev whatsapp.PayloadEvent, rec any) string {
	sum := sha1.Sum(mustJSON(rec))
	return fmt.Sprintf("%s:%s:bad:e%d.c%d.i%d:%s",
		waEventKeyPrefix, kindAbbr, ev.EntryIdx, ev.ChangeIdx, ev.ItemIdx, hex.EncodeToString(sum[:])[:12])
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}

// waEventOutcome is the executor's classification of one claimed unit.
type waEventOutcome struct {
	state     eventlog.Status
	reason    string
	notBefore time.Time
}

func waEventSucceeded() waEventOutcome { return waEventOutcome{state: eventlog.StatusSucceeded} }
func waEventIgnored(reason string) waEventOutcome {
	return waEventOutcome{state: eventlog.StatusIgnored, reason: reason}
}
func waEventPermanent(reason string) waEventOutcome {
	return waEventOutcome{state: eventlog.StatusPermanent, reason: reason}
}
func waEventRetry(reason string) waEventOutcome {
	return waEventOutcome{state: eventlog.StatusPending, reason: reason}
}
func waEventDefer(notBefore time.Time, reason string) waEventOutcome {
	return waEventOutcome{state: eventlog.StatusDeferred, reason: reason, notBefore: notBefore}
}

// processWhatsAppPayload tracks per-event progress. Every identifiable unit must reach a
// terminal state before the stream entry is acknowledged; a transient failure on any unit
// leaves the whole entry pending, but only the unfinished units re-execute on retry.
func processWhatsAppPayload(ctx context.Context, app *App, urlInboxID int, parsed *whatsapp.WebhookPayload) error {
	store := app.whatsappEvents
	if store == nil {
		// Persistence unavailable (e.g. minimal test harness): keep the historical behaviour.
		return processWhatsAppPayloadLegacy(ctx, app, urlInboxID, parsed)
	}

	events, err := splitWhatsAppBusinessEvents(app, urlInboxID, parsed)
	if err != nil {
		app.lo.Error("error splitting whatsapp delivery into business events", "inbox_id", urlInboxID, "error", err)
		return err
	}

	pending := 0
	for _, ev := range events {
		row, claimed, done := waEnsureAndClaim(ctx, app, store, ev)
		if done {
			continue // already terminal on a prior delivery/reclaim
		}
		if !claimed {
			pending++
			continue
		}
		result := executeWhatsAppBusinessEvent(ctx, app, row)
		if err := storePersistOutcome(ctx, app, store, row, result); err != nil {
			pending++
			continue
		}
		if !result.state.Terminal() {
			pending++
		}
	}

	if pending > 0 {
		return fmt.Errorf("%d whatsapp business event unit(s) not yet terminal, leaving delivery pending", pending)
	}
	return nil
}

// waEnsureAndClaim upserts the unit row and leases it for this worker.
// done=true means the unit is already terminal and must be skipped.
// claimed=false (done=false) means the unit stays unfinished (claim lost, deferred
// not_before, or a progress-store error).
func waEnsureAndClaim(ctx context.Context, app *App, store *eventlog.Store, ev waBusinessEvent) (row *eventlog.Event, claimed bool, done bool) {
	row, err := store.Ensure(ctx, ev.inboxID, ev.kind, ev.keyText, json.RawMessage(mustJSON(ev.record)))
	if err != nil {
		app.lo.Error("error persisting whatsapp business event", "event_key", ev.keyText, "error", err)
		return nil, false, false
	}
	if row.Status.Terminal() {
		return nil, false, true
	}
	claimedRow, ok, err := store.Claim(ctx, row.ID, waLeaseOwner(), whatsAppReclaimMinIdle)
	if err != nil {
		app.lo.Error("error claiming whatsapp business event", "event_key", ev.keyText, "error", err)
		return nil, false, false
	}
	if !ok {
		// Another live worker owns it, or it is waiting for not_before.
		return nil, false, false
	}
	return claimedRow, true, false
}

// storePersistOutcome applies the executor result. A lost lease on a now-terminal row is
// harmless; any other marking failure keeps the unit unfinished.
func storePersistOutcome(ctx context.Context, app *App, store *eventlog.Store, row *eventlog.Event, o waEventOutcome) error {
	owner := row.LeaseOwner.String
	var err error
	switch o.state {
	case eventlog.StatusSucceeded:
		err = store.MarkSucceeded(ctx, row.ID, owner)
	case eventlog.StatusIgnored:
		app.lo.Info("whatsapp business event ignored", "event_key", row.EventKey, "kind", row.Kind, "reason", o.reason)
		err = store.MarkIgnored(ctx, row.ID, o.reason, owner)
	case eventlog.StatusPermanent:
		app.lo.Error("whatsapp business event permanently unprocessable", "event_key", row.EventKey, "kind", row.Kind, "reason", o.reason)
		err = store.MarkPermanent(ctx, row.ID, o.reason, owner)
	case eventlog.StatusDeferred:
		app.lo.Info("whatsapp business event deferred", "event_key", row.EventKey, "kind", row.Kind, "reason", o.reason)
		err = store.MarkDeferred(ctx, row.ID, o.notBefore, o.reason, owner)
	default:
		app.lo.Error("whatsapp business event failed transiently", "event_key", row.EventKey, "kind", row.Kind, "attempt", row.Attempts, "error", o.reason)
		err = store.MarkRetry(ctx, row.ID, o.reason, owner)
	}
	if errors.Is(err, eventlog.ErrLeaseLost) {
		if current, getErr := store.Get(ctx, row.ID); getErr == nil && current.Status.Terminal() {
			return nil
		}
	}
	return err
}

// executeWhatsAppBusinessEvent runs one claimed unit and classifies the result.
func executeWhatsAppBusinessEvent(ctx context.Context, app *App, row *eventlog.Event) waEventOutcome {
	switch row.Kind {
	case eventlog.KindMessage:
		return executeWhatsAppMessageEvent(ctx, app, row)
	case eventlog.KindStatus:
		return executeWhatsAppStatusEvent(ctx, app, row)
	case eventlog.KindTemplate:
		return executeWhatsAppTemplateEvent(ctx, app, row)
	default:
		return waEventPermanent(fmt.Sprintf("unknown whatsapp event kind %q", row.Kind))
	}
}

func executeWhatsAppMessageEvent(ctx context.Context, app *App, row *eventlog.Event) waEventOutcome {
	var rec waInboundMessageRecord
	if err := json.Unmarshal(row.Payload, &rec); err != nil {
		return waEventPermanent("decoding message event payload: " + err.Error())
	}
	if rec.Message.ID == "" || rec.Message.From == "" {
		return waEventPermanent("message is missing its id or sender")
	}
	if err := ingestWhatsAppMessage(ctx, app, rec.URLInboxID, rec.Message); err != nil {
		return waEventRetry(err.Error())
	}
	return waEventSucceeded()
}

func executeWhatsAppStatusEvent(ctx context.Context, app *App, row *eventlog.Event) waEventOutcome {
	var rec waInboundStatusRecord
	if err := json.Unmarshal(row.Payload, &rec); err != nil {
		return waEventPermanent("decoding status event payload: " + err.Error())
	}
	st := rec.Status
	if st.MessageID == "" || st.Status == "" {
		return waEventPermanent("status receipt is missing its message id or status")
	}

	_, err := app.conversation.ApplyWhatsAppStatus(st.MessageID, st.Status, st.Timestamp, st.UserMsg)
	if err == nil {
		return waEventSucceeded()
	}
	if errors.Is(err, conversation.ErrMessageNotFound) {
		// The outbound/inbound row can lag the receipt. Wait inside the existing grace window;
		// older events reference a wamid that will never exist locally and are dropped.
		if !st.Timestamp.IsZero() && time.Since(st.Timestamp) > whatsAppStatusNotFoundGrace {
			return waEventIgnored(fmt.Sprintf("status for unknown message %s past grace window", st.MessageID))
		}
		return waEventDefer(time.Now().Add(whatsAppReclaimMinIdle),
			fmt.Sprintf("message %s not found yet, waiting within grace window", st.MessageID))
	}
	return waEventRetry(err.Error())
}

func executeWhatsAppTemplateEvent(ctx context.Context, app *App, row *eventlog.Event) waEventOutcome {
	var rec waTemplateStatusRecord
	if err := json.Unmarshal(row.Payload, &rec); err != nil {
		return waEventPermanent("decoding template event payload: " + err.Error())
	}
	if rec.Update.MetaTemplateID == "" && rec.Update.TemplateName == "" {
		return waEventPermanent("template status update is missing both meta template id and name")
	}
	if rec.TargetInboxID == 0 {
		return waEventIgnored("no enabled whatsapp inbox for the template status update")
	}
	if app.whatsappTemplate == nil {
		return waEventIgnored("whatsapp template manager unavailable")
	}

	u := rec.Update
	if err := app.whatsappTemplate.HandleStatusUpdate(rec.TargetInboxID, u.MetaTemplateID, u.TemplateName, u.Language, u.Event, u.Reason); err != nil {
		return waEventRetry(err.Error())
	}
	// An edit skipped while the reserved CSAT template was under review can now be applied.
	if u.TemplateName == wtmodels.CSATTemplateName(rec.TargetInboxID) {
		go ensureWhatsAppCSATTemplate(app, rec.TargetInboxID)
	}
	return waEventSucceeded()
}

// processWhatsAppPayloadLegacy is the pre-progress-table processing path, retained for
// deployments/tests where the event store is unavailable. A processing error retries the
// whole delivery; all handlers are idempotent via source_id and monotonic status guards.
func processWhatsAppPayloadLegacy(ctx context.Context, app *App, inboxID int, payload *whatsapp.WebhookPayload) error {
	var errs []error

	for _, msg := range payload.ExtractMessages() {
		if err := ingestWhatsAppMessage(ctx, app, inboxID, msg); err != nil {
			app.lo.Error("error ingesting whatsapp message", "inbox_id", inboxID, "wa_message_id", msg.ID, "error", err)
			errs = append(errs, err)
		}
	}

	for _, st := range payload.ExtractStatuses() {
		if _, err := app.conversation.ApplyWhatsAppStatus(st.MessageID, st.Status, st.Timestamp, st.UserMsg); err != nil {
			if errors.Is(err, conversation.ErrMessageNotFound) && time.Since(st.Timestamp) > whatsAppStatusNotFoundGrace {
				app.lo.Warn("dropping whatsapp status for unknown message", "wa_message_id", st.MessageID, "status", st.Status, "event_at", st.Timestamp)
			} else {
				app.lo.Error("error applying whatsapp status update", "wa_message_id", st.MessageID, "status", st.Status, "error", err)
				errs = append(errs, err)
			}
		}
	}

	templateUpdates := payload.ExtractTemplateStatusUpdates()
	if app.whatsappTemplate != nil && len(templateUpdates) > 0 {
		wabaInboxes, err := whatsAppInboxIDsByWABA(app)
		if err != nil {
			app.lo.Error("error listing inboxes for template status updates", "error", err)
			errs = append(errs, err)
			wabaInboxes = map[string][]int{}
		}
		for _, ts := range templateUpdates {
			ids := wabaInboxes[ts.WABAID]
			if ts.WABAID == "" {
				ids = []int{inboxID}
			}
			for _, ibID := range ids {
				if err := app.whatsappTemplate.HandleStatusUpdate(ibID, ts.MetaTemplateID, ts.TemplateName, ts.Language, ts.Event, ts.Reason); err != nil {
					app.lo.Error("error applying template status update", "inbox_id", ibID, "name", ts.TemplateName, "event", ts.Event, "error", err)
					errs = append(errs, err)
					continue
				}
				// An edit skipped while the template was under review can now be applied.
				if ts.TemplateName == wtmodels.CSATTemplateName(ibID) {
					go ensureWhatsAppCSATTemplate(app, ibID)
				}
			}
		}
	}

	return errors.Join(errs...)
}

// waLeaseOwner identifies this process in event leases (diagnostics + stale-lease takeover).
var (
	waLeaseOwnerOnce sync.Once
	waLeaseOwnerVal  string
)

func waLeaseOwner() string {
	waLeaseOwnerOnce.Do(func() {
		host, _ := os.Hostname()
		suffix := make([]byte, 4)
		if _, err := rand.Read(suffix); err != nil {
			waLeaseOwnerVal = fmt.Sprintf("%s:%d", host, os.Getpid())
			return
		}
		waLeaseOwnerVal = fmt.Sprintf("%s:%d:%s", host, os.Getpid(), hex.EncodeToString(suffix))
	})
	return waLeaseOwnerVal
}
