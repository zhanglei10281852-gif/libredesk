package migrations

import (
	"testing"

	"github.com/abhinavxd/libredesk/internal/testutil"
)

func TestV2_9_0_RC11LeaseMigration(t *testing.T) {
	db := testutil.NewDB(t, "migration_v2_9_0_rc11")

	// Idempotent on a fresh schema (value, columns and indexes already exist).
	for range 2 {
		if err := V2_9_0_RC11(db, nil, nil); err != nil {
			t.Fatalf("migration must be idempotent: %v", err)
		}
	}

	for _, column := range []string{"send_generation", "send_owner", "send_lease_until", "send_started_at"} {
		var exists bool
		if err := db.Get(&exists,
			`SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'conversation_messages' AND column_name = $1)`,
			column); err != nil {
			t.Fatalf("checking column %s: %v", column, err)
		}
		if !exists {
			t.Fatalf("column %s must exist after migration", column)
		}
	}

	var hasUnknown bool
	if err := db.Get(&hasUnknown, `SELECT EXISTS (SELECT 1 FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid WHERE t.typname = 'message_status' AND e.enumlabel = 'unknown')`); err != nil {
		t.Fatalf("checking enum value: %v", err)
	}
	if !hasUnknown {
		t.Fatal("message_status enum must gain 'unknown'")
	}

	// Dropping the additions and re-running must restore them. Drop indexes
	// before columns, otherwise the dispatch index disappears with its column.
	db.MustExec(`
		DROP INDEX index_conversation_messages_on_outgoing_head;
		DROP INDEX index_conversation_messages_on_dispatch_lease;
		ALTER TABLE conversation_messages DROP COLUMN send_generation;
		ALTER TABLE conversation_messages DROP COLUMN send_owner;
		ALTER TABLE conversation_messages DROP COLUMN send_lease_until;
		ALTER TABLE conversation_messages DROP COLUMN send_started_at;
	`)
	if err := V2_9_0_RC11(db, nil, nil); err != nil {
		t.Fatalf("re-running migration: %v", err)
	}
	var restored int
	if err := db.Get(&restored, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_name = 'conversation_messages'
		  AND column_name IN ('send_generation', 'send_owner', 'send_lease_until', 'send_started_at')`); err != nil {
		t.Fatalf("verifying restored columns: %v", err)
	}
	if restored != 4 {
		t.Fatalf("expected 4 restored columns, got %d", restored)
	}

	// Existing rows default to generation 1 and stay dispatchable.
	db.MustExec(`INSERT INTO users (type, email, first_name, last_name) VALUES ('contact', 'migr-c@example.test', 'C', '')`)
	db.MustExec(`INSERT INTO users (type, email, first_name, last_name) VALUES ('agent', 'migr@example.test', 'A', '')`)
	db.MustExec(`INSERT INTO inboxes (channel, name) VALUES ('email', 'm')`)
	db.MustExec(`INSERT INTO conversations (contact_id, inbox_id, status_id)
		SELECT (SELECT id FROM users WHERE email='migr-c@example.test'), (SELECT id FROM inboxes WHERE name='m'), id FROM conversation_statuses LIMIT 1`)
	var gen int64
	db.MustExec(`INSERT INTO conversation_messages (type, status, conversation_id, content, text_content, content_type, sender_id, sender_type, private)
		VALUES ('outgoing', 'pending', 1, 'hi', 'hi', 'text', (SELECT id FROM users WHERE email='migr@example.test'), 'agent', false)`)
	if err := db.Get(&gen, `SELECT send_generation FROM conversation_messages ORDER BY id DESC LIMIT 1`); err != nil {
		t.Fatalf("reading generation: %v", err)
	}
	if gen != 1 {
		t.Fatalf("existing messages must default to generation 1, got %d", gen)
	}
}
