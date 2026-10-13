package conversation

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	amodels "github.com/abhinavxd/libredesk/internal/automation/models"
	"github.com/abhinavxd/libredesk/internal/conversation/models"
	whatsappChannel "github.com/abhinavxd/libredesk/internal/inbox/channel/whatsapp"
	"github.com/abhinavxd/libredesk/internal/sla"
	umodels "github.com/abhinavxd/libredesk/internal/user/models"
	wmodels "github.com/abhinavxd/libredesk/internal/webhook/models"
	"github.com/google/uuid"
)

const (
	// outgoingLeaseTTL is how long an owner holds a generation between
	// heartbeats. It is short so a dead process is taken over quickly; the
	// heartbeat extends it across long uploads and channel calls.
	outgoingLeaseTTL = 60 * time.Second
	// outgoingLeaseInterval is the SQL interval literal matching the TTL.
	outgoingLeaseInterval = "60 seconds"
	// Heartbeats run well inside the TTL; a healthy owner never loses a lease.
	outgoingLeaseHeartbeatInterval = 20 * time.Second
	// Dead leases are cheap to detect; run recovery a few times per TTL.
	outgoingRecoveryScanInterval = 15 * time.Second

	// sendUnknownReasonDispatchInterrupted marks a dead owner around the
	// channel call (discovered by the recovery scan).
	sendUnknownReasonDispatchInterrupted = "dispatch_interrupted"
	// sendUnknownReasonChannelUncertain marks a live channel call whose
	// error cannot rule out provider acceptance.
	sendUnknownReasonChannelUncertain = "channel_result_uncertain"

	// Max error text retained on an unknown message for observability.
	sendUnknownDetailMaxLen = 1000
)

// terminalMessageRow is the shape returned by the terminal CAS and the
// interrupted-dispatch recovery query.
type terminalMessageRow struct {
	ID               int             `db:"id"`
	UUID             string          `db:"uuid"`
	Status           string          `db:"status"`
	Meta             json.RawMessage `db:"meta"`
	SenderID         int             `db:"sender_id"`
	ConversationID   int             `db:"conversation_id"`
	ConversationUUID string          `db:"conversation_uuid"`
}

// newLeaseOwner mints a globally unique lease owner token.
func newLeaseOwner() string {
	return uuid.NewString()
}

// startLeaseHeartbeat keeps the owner's lease fresh until the returned stop
// func is invoked. A failed renewal means ownership was lost; it only stops
// renewing, the terminal CAS is what actually blocks stray side effects.
func (m *Manager) startLeaseHeartbeat(messageUUID string, generation int64, owner string) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(outgoingLeaseHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				res, err := m.q.RenewOutgoingMessageLease.Exec(messageUUID, generation, outgoingLeaseInterval, owner)
				if err != nil {
					m.lo.Error("error renewing outgoing message lease", "message_uuid", messageUUID, "generation", generation, "error", err)
					return
				}
				if n, _ := res.RowsAffected(); n == 0 {
					m.lo.Warn("outgoing message lease lost, abandoning side effects", "message_uuid", messageUUID, "generation", generation)
					return
				}
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}

// commitTerminal is the ONLY path through which a dispatching owner finalizes
// a message generation. The UPDATE is a compare-and-swap on (generation,
// owner, status='pending'): whichever committer wins performs the terminal
// side effects exactly once; lease races, duplicate completion, stale owners
// and restart recovery all no-op.
func (m *Manager) commitTerminal(message models.Message, owner, status, sourceID string, metaPatch json.RawMessage) {
	if len(metaPatch) == 0 {
		metaPatch = json.RawMessage(`{}`)
	}
	var row terminalMessageRow
	err := m.q.CommitOutgoingTerminal.Get(&row,
		message.UUID, message.SendGeneration, owner, status, sourceID, []byte(metaPatch))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			m.lo.Info("outgoing terminal commit ignored, no longer the current owner",
				"message_uuid", message.UUID, "generation", message.SendGeneration, "status", status)
		} else {
			m.lo.Error("error committing outgoing message status", "message_uuid", message.UUID, "status", status, "error", err)
		}
		return
	}
	m.afterTerminal(row, metaPatch)
}

// afterTerminal runs the one-shot side effects of an effective terminal
// transition: status broadcast and the message_updated webhook for every
// terminal status, plus reply timestamps, SLA and automation for an effective
// human reply.
func (m *Manager) afterTerminal(row terminalMessageRow, metaPatch json.RawMessage) {
	data := map[string]any{"status": row.Status}
	if len(metaPatch) > 0 && string(metaPatch) != "{}" {
		data["meta"] = stripCSATUUID(row.Meta)
	}
	m.BroadcastMessageUpdate(row.ConversationUUID, row.UUID, data)

	if message, err := m.GetMessage(row.UUID); err != nil {
		m.lo.Error("error fetching message for webhook event", "uuid", row.UUID, "error", err)
	} else {
		m.webhookStore.TriggerEvent(wmodels.EventMessageUpdated, message)
	}

	if row.Status == models.MessageStatusSent {
		m.afterOutgoingSent(row)
	}
}

// afterOutgoingSent advances reply timestamps, SLA and automation once for a
// confirmed human reply, regardless of whether the worker or a channel
// receipt converged the row.
func (m *Manager) afterOutgoingSent(row terminalMessageRow) {
	// Skip system user replies since we only update timestamps and SLA for human replies.
	systemUser, err := m.userStore.GetSystemUser()
	if err != nil {
		m.lo.Error("error fetching system user", "error", err)
		return
	}
	if row.SenderID == systemUser.ID {
		return
	}

	conversation, err := m.GetConversation(row.ConversationID, "", "")
	if err != nil {
		m.lo.Error("error fetching conversation", "conversation_id", row.ConversationID, "error", err)
		return
	}

	now := time.Now()
	nowStr := now.Format(time.RFC3339)
	wsData := map[string]any{"last_reply_at": nowStr, "waiting_since": nil}

	var isFirstReply bool
	if err := m.q.UpdateConversationReplyTimestamps.QueryRow(row.ConversationID, now).Scan(&isFirstReply); err != nil {
		m.lo.Error("error updating conversation reply timestamps", "error", err)
	} else if isFirstReply {
		wsData["first_reply_at"] = nowStr
		// Stamp the first-response SLA immediately.
		if err := m.slaStore.EvaluateConversationSLA(row.ConversationID); err != nil {
			m.lo.Error("error evaluating SLA after first reply", "conversation_id", row.ConversationID, "error", err)
		}
	}

	// Mark latest SLA event for next response metric as met.
	metAt, err := m.slaStore.SetLatestSLAEventMetAt(conversation.AppliedSLAID.Int, sla.MetricNextResponse)
	if err != nil && !errors.Is(err, sla.ErrLatestSLAEventNotFound) {
		m.lo.Error("error setting next response SLA event `met_at`", "conversation_id", conversation.ID, "metric", sla.MetricNextResponse, "applied_sla_id", conversation.AppliedSLAID.Int, "error", err)
	} else if !metAt.IsZero() {
		wsData["next_response_met_at"] = metAt.Format(time.RFC3339)
	}

	m.BroadcastConversationUpdate(row.ConversationUUID, wsData)

	automationMessage := models.Message{SenderID: row.SenderID, Meta: row.Meta}
	if automationMessage.ShouldEvaluateAutomation(systemUser.ID) {
		m.automation.EvaluateConversationUpdateRulesByID(row.ConversationID, "", amodels.EventConversationMessageOutgoing, umodels.User{ID: row.SenderID})
	}
}

// recoverInterruptedDispatches moves pending rows whose owner stamped the
// dispatch-start marker and then let the lease expire into the observable
// 'unknown' state. The channel may have accepted those messages, so they are
// never blindly resent; a stored provider id lets a later channel receipt
// converge them, otherwise they wait for an explicit manual retry.
func (m *Manager) recoverInterruptedDispatches() {
	patch := map[string]any{
		"send_unknown_at":     time.Now().UTC().Format(time.RFC3339),
		"send_unknown_reason": sendUnknownReasonDispatchInterrupted,
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		m.lo.Error("error marshalling interrupted dispatch patch", "error", err)
		return
	}

	var rows []terminalMessageRow
	if err := m.q.RecoverInterruptedDispatches.Select(&rows, []byte(patchBytes)); err != nil {
		m.lo.Error("error recovering interrupted dispatches", "error", err)
		return
	}
	for i := range rows {
		m.lo.Warn("message parked as unknown after interrupted dispatch",
			"message_uuid", rows[i].UUID, "conversation_id", rows[i].ConversationID)
		m.afterTerminal(rows[i], patchBytes)
	}
}

// isDeliveryUncertain reports whether a channel Send error leaves any chance
// the provider already accepted the message. Such errors must converge to the
// unknown state instead of a resendable failure:
//   - channels explicitly flag an ambiguous outcome (WhatsApp after exhausting
//     retries of 429/5xx or losing the transport mid-call);
//   - generic transport-level timeouts and dropped connections are ambiguous,
//     while a rejected connection or a parsed provider rejection is not.
func isDeliveryUncertain(channel string, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, whatsappChannel.ErrDeliveryUncertain) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

// unknownMetaPatch records why a live channel call could not be confirmed.
func unknownMetaPatch(err error) json.RawMessage {
	detail := err.Error()
	if len(detail) > sendUnknownDetailMaxLen {
		detail = detail[:sendUnknownDetailMaxLen]
	}
	patch := map[string]any{
		"send_unknown_at":     time.Now().UTC().Format(time.RFC3339),
		"send_unknown_reason": sendUnknownReasonChannelUncertain,
		"send_unknown_detail": detail,
	}
	raw, marshalErr := json.Marshal(patch)
	if marshalErr != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}

// whatsAppFailureMetaPatch mirrors the provider failure meta previously
// recorded before a failed commit.
func whatsAppFailureMetaPatch(errorMsg string) json.RawMessage {
	errorMsg = strings.TrimSpace(errorMsg)
	patch := map[string]any{
		"provider_status":    WhatsAppStatusFailed,
		"provider_failed_at": time.Now().UTC().Format(time.RFC3339),
	}
	if errorMsg != "" {
		patch["provider_failure_reason"] = errorMsg
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}
