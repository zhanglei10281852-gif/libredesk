-- name: ensure-whatsapp-event
-- Insert a fresh unit, or return the existing row for a redelivery/reclaim. Touching updated_at
-- records the latest observation without changing the processing state.
INSERT INTO whatsapp_inbound_events (event_key, inbox_id, kind, payload)
VALUES ($1, $2, $3, $4)
ON CONFLICT (event_key) DO UPDATE SET updated_at = NOW()
RETURNING id, created_at, updated_at, processed_at, event_key, inbox_id, kind, payload,
          status, attempts, last_error, last_error_at, not_before, lease_owner, lease_at;

-- name: get-whatsapp-event
SELECT id, created_at, updated_at, processed_at, event_key, inbox_id, kind, payload,
       status, attempts, last_error, last_error_at, not_before, lease_owner, lease_at
FROM whatsapp_inbound_events
WHERE id = $1;

-- name: claim-whatsapp-event
-- Only one worker may own a unit: pending/deferred units are claimed immediately, a stale
-- processing lease (dead worker, past the stream reclaim idle) may be taken over. Units whose
-- not_before has not elapsed (e.g. a status waiting for its message) stay put.
UPDATE whatsapp_inbound_events
SET status = 'processing',
    attempts = attempts + 1,
    lease_owner = $2,
    lease_at = NOW(),
    updated_at = NOW()
WHERE id = $1
  AND (
        status IN ('pending', 'deferred')
        OR (status = 'processing' AND lease_at < NOW() - $3::interval)
      )
  AND (not_before IS NULL OR not_before <= NOW())
RETURNING id, created_at, updated_at, processed_at, event_key, inbox_id, kind, payload,
          status, attempts, last_error, last_error_at, not_before, lease_owner, lease_at;

-- name: succeed-whatsapp-event
UPDATE whatsapp_inbound_events
SET status = 'succeeded', last_error = NULL, processed_at = NOW(), lease_owner = NULL, updated_at = NOW()
WHERE id = $1 AND status = 'processing' AND lease_owner = $2;

-- name: ignore-whatsapp-event
UPDATE whatsapp_inbound_events
SET status = 'ignored', last_error = $2, processed_at = NOW(), lease_owner = NULL, updated_at = NOW()
WHERE id = $1 AND status = 'processing' AND lease_owner = $3;

-- name: permfail-whatsapp-event
UPDATE whatsapp_inbound_events
SET status = 'permanent', last_error = $2, last_error_at = NOW(), processed_at = NOW(), lease_owner = NULL, updated_at = NOW()
WHERE id = $1 AND status = 'processing' AND lease_owner = $3;

-- name: retry-whatsapp-event
UPDATE whatsapp_inbound_events
SET status = 'pending', last_error = $2, last_error_at = NOW(), not_before = NULL, lease_owner = NULL, updated_at = NOW()
WHERE id = $1 AND status = 'processing' AND lease_owner = $3;

-- name: defer-whatsapp-event
UPDATE whatsapp_inbound_events
SET status = 'deferred', last_error = $2, last_error_at = NOW(), not_before = $3, lease_owner = NULL, updated_at = NOW()
WHERE id = $1 AND status = 'processing' AND lease_owner = $4;

-- name: prune-terminal-whatsapp-events
DELETE FROM whatsapp_inbound_events
WHERE (status IN ('succeeded', 'ignored') AND processed_at IS NOT NULL AND processed_at < NOW() - $1::interval)
   OR (status = 'permanent' AND processed_at IS NOT NULL AND processed_at < NOW() - $2::interval);
