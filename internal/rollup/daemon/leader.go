package daemon

import (
	"context"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
)

// tryLead takes the leader lock of every rollup the file declares, in name
// order, on conn's session. It leads only with all of them: a daemon that
// holds some gives them back, so two daemons for one file never split its
// work. A session lock lasts as long as the session, so a leader that loses
// its connection loses its locks, and a standby can take them.
func tryLead(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf) (bool, error) {
	var names []string
	for _, r := range conf.Rollups {
		names = append(names, r.Name)
	}
	slices.Sort(names)
	for _, n := range names {
		var got bool
		if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtextextended('sqlflow_rollup_leader:' || $1, 0))", n).Scan(&got); err != nil {
			return false, err
		}
		if !got {
			_, err := conn.Exec(ctx, "SELECT pg_advisory_unlock_all()")
			return false, err
		}
	}
	return true, nil
}
