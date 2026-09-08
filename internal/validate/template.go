package validate

import (
	"sort"
	"strings"

	"github.com/nikolalohinski/gonja/v2"
	"github.com/nikolalohinski/gonja/v2/nodes"
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
		// One diagnostic per name, at its first use. Reading a variable three
		// times is one mistake, not three.
		if seen[r.name] {
			continue
		}
		seen[r.name] = true
		missing = append(missing, r.name)

		rep.Add(diagnostic(
			errs.CodeConfigTemplateUndefined,
			SeverityError,
			"template variable "+r.name+" is not defined and renders as an empty string",
			&Position{Source: "config", Line: r.line, Column: r.col},
			nearest(r.name, provided)...,
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
		for name := range provided {
			if !referenced[name] {
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

	if len(missing) > 0 {
		rep.SetCheck("config.template", StatusFail, "")
	} else {
		rep.SetCheck("config.template", StatusPass, "")
	}
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
type ref struct {
	name string
	line int
	col  int
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
		w.walk(v.Expression)
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
		w.refs = append(w.refs, ref{v.Name.Val, v.Name.Line, v.Name.Col})

	case *nodes.Variable:
		// Only the root of a dotted lookup is a variable. The rest are
		// attributes of whatever it resolves to.
		if len(v.Parts) > 0 {
			w.refs = append(w.refs, ref{v.Parts[0].S, v.Location.Line, v.Location.Col})
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
