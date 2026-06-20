// Package migrations embeds the SQL schema migrations for both databases so the
// manager can apply them on startup without files on disk.
package migrations

import "embed"

// FS holds the Postgres and ClickHouse migration SQL.
//
//go:embed postgres/*.sql clickhouse/*.sql
var FS embed.FS
