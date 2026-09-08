package validate

import (
	"github.com/nikolalohinski/gonja/v2"
	"github.com/nikolalohinski/gonja/v2/nodes"
)

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
