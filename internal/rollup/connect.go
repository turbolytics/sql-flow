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
	return connect(ctx, dsn, "store.postgres.dsn", "rollup store")
}

// ConnectDSN opens a connection to --dsn, the database `sqlflow rollup test`
// runs in. Its errors name the flag, since the command never reads the
// store.
func ConnectDSN(ctx context.Context, dsn string) (*pgx.Conn, error) {
	return connect(ctx, dsn, "--dsn", "--dsn")
}

// connect is Connect with the setting's name for a parse error and the
// label a connect error starts with.
func connect(ctx context.Context, dsn, setting, label string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errs.New(errs.CodeConfigInvalid, "%s is not a Postgres connection string or URL", setting)
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
		return nil, errs.New(errs.CodeRollupUnreachable, "%s %s/%s: %s", label, cfg.Host, cfg.Database, msg)
	}
	return conn, nil
}
