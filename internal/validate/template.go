package validate

import (
	"sort"
	"strings"

	"github.com/nikolalohinski/gonja/v2"
	"github.com/nikolalohinski/gonja/v2/nodes"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// checkTemplate reports the config's template faults into rep.
//
// It never returns early. A config with three missing variables reports three
// diagnostics in one run, because every round trip costs an agent a turn.
func checkTemplate(src string, provided map[string]string, rep *Report) {
	refs, complete, err := scanTemplate(src)
	if err != nil {
		rep.SetCheck("config.template", StatusFail, "")
		rep.Add(diagnostic(errs.CodeConfigParseFailed, SeverityError,
			"the config is not a valid template: "+err.Error(), nil))
		rep.Variables = &Variables{
			Referenced: []string{},
			Provided:   sortedKeys(provided),
			Missing:    []string{},
		}
		return
	}

	referenced := map[string]bool{}
	var missing []string
	seen := map[string]bool{}

	for _, r := range refs {
		referenced[r.name] = true
		if _, ok := provided[r.name]; ok {
			continue
		}
		// A reference with a default filter is meant to be absent.
		if r.optional {
			continue
		}
		// One diagnostic per name, at its first use. Reading a variable three
		// times is one mistake, not three.
		if seen[r.name] {
			continue
		}
		seen[r.name] = true
		missing = append(missing, r.name)

		// A config that reads {{ SQLFLOW_ROOT_DIR }} with no default is
		// declaring a required input. Absent, it is the environment that is
		// incomplete, not the config, and failing on it would make validation
		// useless in CI, where secrets are not set (#142).
		//
		// A missing name that closely resembles a provided one is different.
		// That resemblance is evidence of a typo, which is the fault #120
		// spent days on, so it fails.
		neighbours := nearest(r.name, provided)
		severity := SeverityWarning
		message := "template variable " + r.name +
			" is not set and renders as an empty string"
		if len(neighbours) > 0 {
			severity = SeverityError
			message = "template variable " + r.name +
				" is not defined and renders as an empty string, but a similar name is supplied and never read"
		}

		rep.Add(diagnostic(
			errs.CodeConfigTemplateUndefined,
			severity,
			message,
			&Position{Source: "config", Line: r.line, Column: r.col},
			neighbours...,
		))
	}

	sort.Strings(missing)
	if missing == nil {
		missing = []string{}
	}

	vars := &Variables{
		Referenced: sortedSet(referenced),
		Provided:   sortedKeys(provided),
		Missing:    missing,
	}

	// Only a complete walk can prove a variable goes unread.
	if complete {
		injected := map[string]bool{}
		for _, name := range config.SettingsVarNames() {
			injected[name] = true
		}
		for name := range provided {
			if !referenced[name] && !injected[name] {
				vars.Unused = append(vars.Unused, name)
			}
		}
		sort.Strings(vars.Unused)
		rep.SetCheck("config.template.unused", StatusPass, "")
	} else {
		rep.SetCheck("config.template.unused", StatusSkipped,
			"the template has control structures, so an unread variable cannot be proven")
	}

	rep.Variables = vars

	// A warning is advice, so only an error fails the check.
	status := StatusPass
	for _, d := range rep.Diagnostics {
		if d.Code == string(errs.CodeConfigTemplateUndefined) && d.Severity == SeverityError {
			status = StatusFail
			break
		}
	}
	rep.SetCheck("config.template", status, "")
}

// nearest returns the provided names close enough to want to be plausible
// misspellings, closest first. It is what turns "missing
// SQLFLOW_AZURE_CONNECTION_STRING" into a pointer at the
// SQLFLOW_AZURE_STORAGE_CONNECTION_STRING sitting unread beside it.
func nearest(want string, provided map[string]string) []string {
	type scored struct {
		name string
		dist int
	}

	// Scaled to the name's length. SQLFLOW_ prefixes make every candidate
	// long, so a fixed threshold either matches everything or nothing.
	limit := len(want)/4 + 1

	var candidates []scored
	for name := range provided {
		d := editDistance(strings.ToUpper(want), strings.ToUpper(name))
		if d <= limit {
			candidates = append(candidates, scored{name, d})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].dist != candidates[j].dist {
			return candidates[i].dist < candidates[j].dist
		}
		return candidates[i].name < candidates[j].name
	})

	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		out = append(out, c.name)
	}
	return out
}

func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, min(cur[j-1]+1, prev[j-1]+cost))
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}

// diagnostic builds a Diagnostic, taking Class and Action from the code's
// registry entry so the two cannot drift.
func diagnostic(code errs.Code, sev Severity, msg string, pos *Position, didYouMean ...string) Diagnostic {
	d := Diagnostic{
		Code:       string(code),
		Class:      string(code.Class()),
		Severity:   sev,
		Message:    msg,
		Position:   pos,
		DidYouMean: didYouMean,
	}
	if def, ok := errs.Lookup(code); ok {
		d.Action = def.Action
	}
	return d
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ref is one variable reference, with the position of the name itself.
//
// optional marks a reference the author gave a fallback, as in
// {{ SQLFLOW_KAFKA_BROKERS|default('localhost:9092') }}. Leaving it undefined
// is the documented way to use that config, so reporting it as missing would
// fail every shipped example.
type ref struct {
	name     string
	line     int
	col      int
	optional bool
}

// scanTemplate parses the config as a Jinja2 template and collects every
// variable it reads.
//
// complete reports whether the walk understood every node it met. A control
// structure hides its body and binds its own loop names, and an unmodelled
// expression type could hide a reference. Either sets complete false, which
// suppresses the unused report: a name this walk never saw is not evidence
// that the config does not use it.
//
// The walk is hand-written because gonja's own nodes.Inspect stops at the
// first Data node in v2.9.0 and never reaches an expression.
func scanTemplate(src string) (refs []ref, complete bool, err error) {
	tmpl, err := gonja.FromString(src)
	if err != nil {
		return nil, false, err
	}

	w := walker{complete: true}
	for _, n := range tmpl.Root().Nodes {
		w.walk(n)
	}
	return w.refs, w.complete, nil
}

type walker struct {
	refs     []ref
	complete bool

	// optional counts the default filters enclosing the node being walked.
	// A reference inside one has a fallback and cannot be missing.
	optional int
}

func (w *walker) walk(n nodes.Node) {
	if n == nil {
		return
	}

	switch v := n.(type) {
	case *nodes.Data, *nodes.Comment, *nodes.String, *nodes.Integer,
		*nodes.Float, *nodes.Bool, *nodes.None:
		// Literals read nothing.

	case *nodes.Output:
		w.walk(v.Expression)
		w.walk(v.Condition)
		w.walk(v.Alternative)

	case *nodes.FilteredExpression:
		// A default filter supplies the value when the variable is absent,
		// so the reference under it is optional.
		defaulted := false
		for _, f := range v.Filters {
			if f.Name == "default" {
				defaulted = true
				break
			}
		}
		if defaulted {
			w.optional++
		}
		w.walk(v.Expression)
		if defaulted {
			w.optional--
		}

		// The filter's own arguments are not covered by the fallback.
		for _, f := range v.Filters {
			for _, a := range f.Args {
				w.walk(a)
			}
			for _, a := range f.Kwargs {
				w.walk(a)
			}
		}

	case *nodes.TestExpression:
		w.walk(v.Expression)

	case *nodes.Name:
		w.refs = append(w.refs, ref{v.Name.Val, v.Name.Line, v.Name.Col, w.optional > 0})

	case *nodes.Variable:
		// Only the root of a dotted lookup is a variable. The rest are
		// attributes of whatever it resolves to.
		if len(v.Parts) > 0 {
			w.refs = append(w.refs,
				ref{v.Parts[0].S, v.Location.Line, v.Location.Col, w.optional > 0})
		}

	case *nodes.GetAttribute:
		w.walk(v.Node)

	case *nodes.GetItem:
		w.walk(v.Node)
		w.walk(v.Arg)

	case *nodes.Call:
		w.walk(v.Func)
		for _, a := range v.Args {
			w.walk(a)
		}
		for _, a := range v.Kwargs {
			w.walk(a)
		}

	case *nodes.BinaryExpression:
		w.walk(v.Left)
		w.walk(v.Right)

	case *nodes.UnaryExpression:
		w.walk(v.Term)

	case *nodes.Negation:
		w.walk(v.Term)

	case *nodes.List:
		for _, e := range v.Val {
			w.walk(e)
		}

	case *nodes.Tuple:
		for _, e := range v.Val {
			w.walk(e)
		}

	case *nodes.Dict:
		for _, p := range v.Pairs {
			w.walk(p.Key)
			w.walk(p.Value)
		}

	default:
		// A node this walk does not model, a control structure above all.
		// Recording the gap is the honest response; guessing produces a
		// false unused report, and a model acts on those.
		w.complete = false
	}
}
