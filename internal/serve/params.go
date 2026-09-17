package serve

import (
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
)

// apiError is a request the server refuses. It becomes the one error shape:
// {"error": {"code", "message"}}.
type apiError struct {
	status  int
	code    string
	message string
}

// resolveStatement picks the dataset's statement for the request's grain.
func (ds *dataset) resolveStatement(query url.Values) (datasetStatement, *apiError) {
	grains, given := query["grain"]

	if ds.grains == nil {
		if given {
			return datasetStatement{}, &apiError{http.StatusBadRequest, "unknown_grain",
				"dataset " + ds.conf.Name + " has no grains"}
		}
		return ds.single, nil
	}

	names := ds.conf.GrainNames()
	switch {
	case !given:
		return datasetStatement{}, &apiError{http.StatusBadRequest, "missing_grain",
			"dataset " + ds.conf.Name + " needs a grain; grains: " + strings.Join(names, ", ")}
	case len(grains) > 1:
		return datasetStatement{}, &apiError{http.StatusBadRequest, "invalid_param",
			"grain is given " + strconv.Itoa(len(grains)) + " times"}
	}

	st, ok := ds.grains[grains[0]]
	if !ok {
		return datasetStatement{}, &apiError{http.StatusBadRequest, "unknown_grain",
			"dataset " + ds.conf.Name + " has no grain " + grains[0] + "; grains: " + strings.Join(names, ", ")}
	}
	return st, nil
}

// window is the range a ranged request resolved to, echoed in the response
// so a caller can draw its axis.
type window struct {
	Since string `json:"since"`
	Until string `json:"until"`
}

// resolveRange fills in a ranged request's since and until, then picks its
// grain: the one named, if it serves the range, or else the finest one that
// does.
//
// values already holds the parsed params. The resolved since and until are
// written back into it, so the statement binds timestamps, never NULL, and
// needs no defaults of its own.
func (ds *dataset) resolveRange(query url.Values, values map[string]any, now time.Time) (datasetStatement, *window, *apiError) {
	sp := ds.span

	// Microseconds, because that is what binds. The echoed range then says
	// exactly what the statement saw.
	until, ok := values[sp.until].(time.Time)
	if !ok {
		until = now
	}
	until = until.UTC().Truncate(time.Microsecond)
	since, ok := values[sp.since].(time.Time)
	if !ok {
		since = until.Add(-sp.def)
	}
	since = since.UTC().Truncate(time.Microsecond)

	if !since.Before(until) {
		return datasetStatement{}, nil, &apiError{http.StatusBadRequest, "invalid_param",
			sp.since + " must be before " + sp.until + "; got " + sp.since + " " + since.Format(time.RFC3339Nano) +
				" and " + sp.until + " " + until.Format(time.RFC3339Nano)}
	}
	values[sp.since], values[sp.until] = since, until
	win := &window{Since: since.Format(time.RFC3339Nano), Until: until.Format(time.RFC3339Nano)}
	width := until.Sub(since)

	grains, given := query["grain"]
	if !given {
		for _, g := range sp.grains {
			if width <= g.max {
				return ds.grains[g.name], win, nil
			}
		}
		widest := sp.grains[len(sp.grains)-1]
		return datasetStatement{}, nil, &apiError{http.StatusBadRequest, "range_too_wide",
			"the range is " + describeWidth(width) + " and the widest grain, " + widest.name +
				", serves at most " + config.FormatServeDuration(widest.max)}
	}

	if len(grains) > 1 {
		return datasetStatement{}, nil, &apiError{http.StatusBadRequest, "invalid_param",
			"grain is given " + strconv.Itoa(len(grains)) + " times"}
	}
	st, ok := ds.grains[grains[0]]
	if !ok {
		return datasetStatement{}, nil, &apiError{http.StatusBadRequest, "unknown_grain",
			"dataset " + ds.conf.Name + " has no grain " + grains[0] + "; grains: " + strings.Join(ds.conf.GrainNames(), ", ")}
	}

	var fits []string
	var max time.Duration
	for _, g := range sp.grains {
		if g.name == grains[0] {
			max = g.max
		}
		if width <= g.max {
			fits = append(fits, g.name)
		}
	}
	if width > max {
		msg := "grain " + grains[0] + " serves at most " + config.FormatServeDuration(max) +
			" and the range is " + describeWidth(width)
		if len(fits) > 0 {
			msg += "; grains that serve it: " + strings.Join(fits, ", ")
		}
		return datasetStatement{}, nil, &apiError{http.StatusBadRequest, "range_too_wide", msg}
	}
	return st, win, nil
}

// describeWidth writes a requested width in whole units when it has them,
// and to the second when it does not: 3d, or 72h0m1s.
func describeWidth(d time.Duration) string {
	if d%time.Second == 0 {
		return config.FormatServeDuration(d)
	}
	return d.Round(time.Second).String()
}

// parseParams parses every query parameter but grain against its declared
// type. An unknown name is refused rather than ignored, so a misspelled
// filter fails instead of silently binding NULL and returning every row.
//
// Names are checked in sorted order, so a request with two faults reports
// the same one every time.
func parseParams(declared []config.ServeParam, query url.Values) (map[string]any, *apiError) {
	params := map[string]config.ServeParam{}
	var names []string
	for _, p := range declared {
		params[p.Name] = p
		names = append(names, p.Name)
	}

	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	values := map[string]any{}
	for _, key := range keys {
		// Neither is the dataset's: grain picks the statement and client_id
		// names the caller. Neither reaches the cache key, so two clients
		// asking one question share one answer.
		if key == "grain" || key == clientIDParam {
			continue
		}
		p, ok := params[key]
		if !ok {
			msg := "no param named " + key
			if len(names) > 0 {
				msg += "; params: " + strings.Join(names, ", ")
			}
			return nil, &apiError{http.StatusBadRequest, "unknown_param", msg}
		}
		raw := query[key]
		if len(raw) > 1 {
			return nil, &apiError{http.StatusBadRequest, "invalid_param",
				"param " + key + " is given " + strconv.Itoa(len(raw)) + " times"}
		}

		v, err := parseValue(p.Type, raw[0])
		if err != nil {
			return nil, &apiError{http.StatusBadRequest, "invalid_param",
				"param " + key + " is " + describe(p.Type) + "; got " + strconv.Quote(raw[0])}
		}
		if n, isInteger := v.(int64); isInteger {
			if msg := boundsProblem(p, n); msg != "" {
				return nil, &apiError{http.StatusBadRequest, "invalid_param", msg}
			}
		}
		values[key] = v
	}
	return values, nil
}

// boundsProblem says why an integer lies outside its param's bounds, or ""
// when it does not.
func boundsProblem(p config.ServeParam, n int64) string {
	format := strconv.FormatInt
	got := "; got " + format(n, 10)
	switch {
	case p.Min != nil && p.Max != nil && (n < *p.Min || n > *p.Max):
		return "param " + p.Name + " must be between " + format(*p.Min, 10) + " and " + format(*p.Max, 10) + got
	case p.Min != nil && n < *p.Min:
		return "param " + p.Name + " must be at least " + format(*p.Min, 10) + got
	case p.Max != nil && n > *p.Max:
		return "param " + p.Name + " must be at most " + format(*p.Max, 10) + got
	}
	return ""
}

func parseValue(typ, raw string) (any, error) {
	switch typ {
	case "integer":
		return strconv.ParseInt(raw, 10, 64)
	case "timestamp":
		return time.Parse(time.RFC3339Nano, raw)
	default:
		return raw, nil
	}
}

func describe(typ string) string {
	switch typ {
	case "integer":
		return "a base-10 integer that fits in 64 bits"
	case "timestamp":
		// A + in a query string decodes as a space, which is the usual way a
		// correct offset arrives broken.
		return "an RFC 3339 timestamp with an offset, such as 2026-09-10T00:00:00Z (encode + as %2B)"
	default:
		return "a string"
	}
}
