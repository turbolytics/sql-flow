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
func (ds *dataset) resolveStatement(query url.Values) (*statement, *apiError) {
	grains, given := query["grain"]

	if ds.grains == nil {
		if given {
			return nil, &apiError{http.StatusBadRequest, "unknown_grain",
				"dataset " + ds.conf.Name + " has no grains"}
		}
		return ds.single, nil
	}

	names := ds.conf.GrainNames()
	switch {
	case !given:
		return nil, &apiError{http.StatusBadRequest, "missing_grain",
			"dataset " + ds.conf.Name + " needs a grain; grains: " + strings.Join(names, ", ")}
	case len(grains) > 1:
		return nil, &apiError{http.StatusBadRequest, "invalid_param",
			"grain is given " + strconv.Itoa(len(grains)) + " times"}
	}

	st, ok := ds.grains[grains[0]]
	if !ok {
		return nil, &apiError{http.StatusBadRequest, "unknown_grain",
			"dataset " + ds.conf.Name + " has no grain " + grains[0] + "; grains: " + strings.Join(names, ", ")}
	}
	return st, nil
}

// parseParams parses every query parameter but grain against its declared
// type. An unknown name is refused rather than ignored, so a misspelled
// filter fails instead of silently binding NULL and returning every row.
//
// Names are checked in sorted order, so a request with two faults reports
// the same one every time.
func parseParams(declared []config.ServeParam, query url.Values) (map[string]any, *apiError) {
	types := map[string]string{}
	var names []string
	for _, p := range declared {
		types[p.Name] = p.Type
		names = append(names, p.Name)
	}

	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	values := map[string]any{}
	for _, key := range keys {
		if key == "grain" {
			continue
		}
		typ, ok := types[key]
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

		v, err := parseValue(typ, raw[0])
		if err != nil {
			return nil, &apiError{http.StatusBadRequest, "invalid_param",
				"param " + key + " is " + describe(typ) + "; got " + strconv.Quote(raw[0])}
		}
		values[key] = v
	}
	return values, nil
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
