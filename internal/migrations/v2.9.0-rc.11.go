package migrations

import (
	"github.com/jmoiron/sqlx"
	"github.com/knadh/koanf/v2"
	"github.com/knadh/stuffbin"
)

// V2_9_0_RC11 adds the durable progress table for inbound WhatsApp webhook business events.
func V2_9_0_RC11(db *sqlx.DB, fs stuffbin.FileSystem, ko *koanf.Koanf) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS whatsapp_inbound_events (
			id BIGSERIAL PRIMARY KEY,
			created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
			updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
			processed_at TIMESTAMPTZ NULL,
			event_key TEXT NOT NULL,
			inbox_id BIGINT NOT NULL,
			kind TEXT NOT NULL,
			payload JSONB DEFAULT '{}'::jsonb NOT NULL,
			status TEXT DEFAULT 'pending' NOT NULL,
			attempts INT DEFAULT 0 NOT NULL,
			last_error TEXT NULL,
			last_error_at TIMESTAMPTZ NULL,
			not_before TIMESTAMPTZ NULL,
			lease_owner TEXT NULL,
			lease_at TIMESTAMPTZ NULL,
			CONSTRAINT constraint_whatsapp_inbound_events_on_kind CHECK (kind IN ('message', 'status', 'template')),
			CONSTRAINT constraint_whatsapp_inbound_events_on_status CHECK (status IN ('pending', 'deferred', 'processing', 'succeeded', 'ignored', 'permanent'))
		);
		CREATE UNIQUE INDEX IF NOT EXISTS index_unique_whatsapp_inbound_events_on_event_key ON whatsapp_inbound_events (event_key);
		CREATE INDEX IF NOT EXISTS index_whatsapp_inbound_events_on_status_updated_at ON whatsapp_inbound_events (status, updated_at);
	`)
	return err
}
