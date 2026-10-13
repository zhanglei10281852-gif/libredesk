package migrations

import (
	"github.com/jmoiron/sqlx"
	"github.com/knadh/koanf/v2"
	"github.com/knadh/stuffbin"
)

// V2_9_0_RC11 introduces per-generation, lease-based exclusive ownership of
// outgoing message dispatch and the `unknown` message status for sends whose
// channel result cannot be determined safely.
func V2_9_0_RC11(db *sqlx.DB, fs stuffbin.FileSystem, ko *koanf.Koanf) error {
	// ALTER TYPE ... ADD VALUE must run outside of a transaction block on
	// older PostgreSQL versions, so it needs its own Exec call.
	if _, err := db.Exec(`ALTER TYPE message_status ADD VALUE IF NOT EXISTS 'unknown';`); err != nil {
		return err
	}

	if _, err := db.Exec(`
		ALTER TABLE conversation_messages ADD COLUMN IF NOT EXISTS send_generation BIGINT NOT NULL DEFAULT 1;
		ALTER TABLE conversation_messages ADD COLUMN IF NOT EXISTS send_owner TEXT NULL;
		ALTER TABLE conversation_messages ADD COLUMN IF NOT EXISTS send_lease_until TIMESTAMPTZ NULL;
		ALTER TABLE conversation_messages ADD COLUMN IF NOT EXISTS send_started_at TIMESTAMPTZ NULL;
	`); err != nil {
		return err
	}

	if _, err := db.Exec(`
		-- Powers the same-conversation ordering guard while dispatching.
		CREATE INDEX IF NOT EXISTS index_conversation_messages_on_outgoing_head
			ON conversation_messages (conversation_id, id)
			WHERE type = 'outgoing' AND private = false AND status IN ('pending', 'unknown');

		-- Powers the interrupted-dispatch recovery scan.
		CREATE INDEX IF NOT EXISTS index_conversation_messages_on_dispatch_lease
			ON conversation_messages (send_lease_until)
			WHERE send_started_at IS NOT NULL;
	`); err != nil {
		return err
	}

	return nil
}
