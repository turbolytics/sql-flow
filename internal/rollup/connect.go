package rollup

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// connectTimeout bounds a connect, so a database that drops packets fails a
// deploy's entrypoint instead of hanging it.
const connectTimeout = 10 * time.Second

// Connect opens a connection to the rollup store. Its errors never carry the
// password: the DSN holds it, so neither the DSN nor pgx's parse error is
// repeated, and a connect error has it removed.
func Connect(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errs.New(errs.CodeConfigInvalid, "store.postgres.dsn is not a Postgres connection string or URL")
	}
	if cfg.ConnectTimeout <= 0 || cfg.ConnectTimeout > connectTimeout {
		cfg.ConnectTimeout = connectTimeout
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		msg := err.Error()
		if cfg.Password != "" {
			msg = strings.ReplaceAll(msg, cfg.Password, "********")
		}
		return nil, errs.New(errs.CodeRollupUnreachable, "rollup store %s/%s: %s", cfg.Host, cfg.Database, msg)
	}
	return conn, nil
}
