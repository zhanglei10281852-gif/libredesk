package migrations

import (
	"github.com/jmoiron/sqlx"
	"github.com/knadh/koanf/v2"
	"github.com/knadh/stuffbin"
)

func V2_9_0_RC11(db *sqlx.DB, fs stuffbin.FileSystem, ko *koanf.Koanf) error {
	// runtime_generation identifies the committed config generation the unique
	// active runtime for an inbox must match. Zero for all pre-existing rows.
	_, err := db.Exec(`
		ALTER TABLE inboxes ADD COLUMN IF NOT EXISTS runtime_generation BIGINT NOT NULL DEFAULT 0;
	`)
	return err
}
