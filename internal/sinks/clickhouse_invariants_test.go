package sinks

// The clickhouse.* invariants: claims only the ClickHouse sink makes, each
// one a sentence on docs/published/clickhouse.mdx. A test here that fails is
// a page that is now wrong. Fix the page or the sink, never the assertion.

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestSinkClickhouse_DSNSchemeDecidesProtocolTLSAndPort(t *testing.T) {
	coverage.Invariant(t, "clickhouse.dsn.scheme", "sink.clickhouse")

	cases := []struct {
		dsn      string
		protocol clickhouse.Protocol
		tls      bool
		addr     string
	}{
		{dsn: "clickhouses://h/db", protocol: clickhouse.HTTP, tls: true, addr: "h:8443"},
		{dsn: "https://h/db", protocol: clickhouse.HTTP, tls: true, addr: "h:8443"},
		{dsn: "clickhouse://h/db", protocol: clickhouse.HTTP, tls: false, addr: "h:8123"},
		{dsn: "http://h/db", protocol: clickhouse.HTTP, tls: false, addr: "h:8123"},
		{dsn: "natives://h/db", protocol: clickhouse.Native, tls: true, addr: "h:9440"},
		{dsn: "tcp://h/db", protocol: clickhouse.Native, tls: false, addr: "h:9000"},
		{dsn: "native://h/db", protocol: clickhouse.Native, tls: false, addr: "h:9000"},
	}
	for _, c := range cases {
		opts, err := clickhouseOptions(c.dsn)
		assert.NoError(t, err)
		assert.Equal(t, c.protocol, opts.Protocol)
		assert.Equal(t, c.tls, opts.TLS != nil)
		assert.DeepEqual(t, []string{c.addr}, opts.Addr)
	}

	// @ and : in a password parse raw.
	opts, err := clickhouseOptions("clickhouses://u:p@ss:w0rd@h/db")
	assert.NoError(t, err)
	assert.Equal(t, "p@ss:w0rd", opts.Auth.Password)

	// # left raw ends the URL early, so what follows reads as a port.
	_, err = clickhouseOptions("clickhouses://u:pa#ss@h/db")
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "invalid port"))

	// Percent-encoded, the same password parses.
	opts, err = clickhouseOptions("clickhouses://u:pa%23ss@h/db")
	assert.NoError(t, err)
	assert.Equal(t, "pa#ss", opts.Auth.Password)
}

func TestSinkClickhouse_TLSVerifiesAgainstTheSystemRoots(t *testing.T) {
	coverage.Invariant(t, "clickhouse.tls.verifies", "sink.clickhouse")

	opts, err := clickhouseOptions("clickhouses://h/db")
	assert.NoError(t, err)
	assert.False(t, opts.TLS.InsecureSkipVerify)
	assert.Nil(t, opts.TLS.RootCAs)

	// No option skips verification: the sink's config is a DSN and a table.
	// A field added here is a page claim to revisit.
	var names []string
	typ := reflect.TypeOf(config.ClickhouseSink{})
	for i := 0; i < typ.NumField(); i++ {
		names = append(names, typ.Field(i).Name)
	}
	assert.DeepEqual(t, []string{"DSN", "Table"}, names)
}
