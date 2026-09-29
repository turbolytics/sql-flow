package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The line shapes a real corpus holds. The NASA traces carry truncated final
// lines, raw control bytes, and requests with no bracketed time at all, and a
// publisher that panics or drops on those loses data the archive is meant to
// keep.
func TestCLFTime(t *testing.T) {
	cases := []struct {
		name string
		line string
		want string // RFC 3339, or "" for a line that carries no usable time
	}{
		{
			name: "a request",
			line: `199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245`,
			want: "1995-07-01T00:00:01-04:00",
		},
		{
			name: "a different zone",
			line: `host - - [31/Aug/1995:23:59:59 +0530] "GET / HTTP/1.0" 200 1`,
			want: "1995-08-31T23:59:59+05:30",
		},
		{
			name: "no brackets",
			line: `this line is not a request at all`,
		},
		{
			name: "truncated mid-timestamp",
			line: `199.72.81.55 - - [01/Jul/1995:00:00`,
		},
		{
			name: "brackets around something else",
			line: `199.72.81.55 - - [not a date] "GET / HTTP/1.0" 200 1`,
		},
		{
			name: "empty",
			line: ``,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := clfTime(c.line)
			if c.want == "" {
				if ok {
					t.Fatalf("read a time from a line that has none: %s", got)
				}
				return
			}
			if !ok {
				t.Fatal("no time read")
			}
			if s := got.Format(time.RFC3339); s != c.want {
				t.Fatalf("got %s, want %s", s, c.want)
			}
		})
	}
}

// The shift is computed from the newest line in the corpus, not the first, so
// that every record lands at or before the run's start. Shifting on the first
// line instead would put the rest of the trace in the future, where the engine
// refuses an event time more than a minute ahead of its own clock.
func TestNewestTimeIsTheLatestAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, lines ...string) string {
		p := filepath.Join(dir, name)
		var b []byte
		for _, l := range lines {
			b = append(b, l...)
			b = append(b, '\n')
		}
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	jul := write("jul",
		`a - - [01/Jul/1995:00:00:01 -0400] "GET / HTTP/1.0" 200 1`,
		`b - - [31/Jul/1995:23:59:59 -0400] "GET / HTTP/1.0" 200 1`,
	)
	aug := write("aug",
		`c - - [01/Aug/1995:00:00:00 -0400] "GET / HTTP/1.0" 200 1`,
		`d - - [15/Aug/1995:12:00:00 -0400] "GET / HTTP/1.0" 200 1`,
		`e - - [not a date] "GET / HTTP/1.0" 200 1`,
	)

	got, err := newestTime([]string{jul, aug})
	if err != nil {
		t.Fatal(err)
	}
	want := "1995-08-15T12:00:00-04:00"
	if s := got.Format(time.RFC3339); s != want {
		t.Fatalf("got %s, want %s", s, want)
	}

	// Ordering of the files must not decide the answer.
	rev, err := newestTime([]string{aug, jul})
	if err != nil {
		t.Fatal(err)
	}
	if !rev.Equal(got) {
		t.Fatalf("file order changed the newest time: %s then %s", got, rev)
	}
}

// A corpus with no parsable timestamp cannot be shifted, and saying so beats
// publishing it silently unshifted against an engine that will refuse it.
func TestNewestTimeRefusesACorpusWithNoTimestamps(t *testing.T) {
	p := filepath.Join(t.TempDir(), "none")
	if err := os.WriteFile(p, []byte("one\ntwo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newestTime([]string{p}); err == nil {
		t.Fatal("expected an error for a corpus carrying no timestamps")
	}
}
