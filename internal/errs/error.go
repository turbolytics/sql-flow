package errs

import (
	"errors"
	"fmt"
)

// Position points at the place in the user's own text that caused a user
// error. `validate` and `preview` return it so a UI can highlight the line
// rather than printing a paragraph.
type Position struct {
	// Source names what Line and Column index into: "sql" or "config".
	Source string
	Line   int
	Column int
}

func (p Position) String() string {
	return fmt.Sprintf("%s:%d:%d", p.Source, p.Line, p.Column)
}

// Error carries a code alongside the message.
//
// Interior call sites keep using fmt.Errorf with %w. Only the places where an
// error becomes observable need a code: the CLI boundary, the log, and the
// `validate` response. The code survives any amount of wrapping in between,
// which is why converting the codebase does not mean touching every raise
// site.
type Error struct {
	Code    Code
	Msg     string
	Pos     *Position
	Wrapped error
}

// Error prefixes the code rather than appending it. A wrapped YAML or SQL
// error runs to many lines, and a trailing code lands below all of them where
// nobody reads it. In front, the code is always the first thing on the line
// and stays greppable.
func (e *Error) Error() string {
	var body string
	switch {
	case e.Msg != "" && e.Wrapped != nil:
		body = e.Msg + ": " + e.Wrapped.Error()
	case e.Msg != "":
		body = e.Msg
	case e.Wrapped != nil:
		body = e.Wrapped.Error()
	}
	if e.Pos != nil {
		body += " at " + e.Pos.String()
	}
	if body == "" {
		return fmt.Sprintf("[%s]", e.Code)
	}
	return fmt.Sprintf("[%s] %s", e.Code, body)
}

func (e *Error) Unwrap() error { return e.Wrapped }

// New builds a coded error with no cause.
func New(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}

// Wrap attaches a code to an existing error. Wrapping an error that already
// carries a code keeps the outer one: the boundary nearest the user decides
// how to describe the failure.
func Wrap(code Code, err error, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...), Wrapped: err}
}

// At records where in the user's text the failure happened.
func (e *Error) At(source string, line, column int) *Error {
	e.Pos = &Position{Source: source, Line: line, Column: column}
	return e
}

// CodeOf returns the code of the outermost coded error in the chain.
//
// An error carrying no code reports as CodeInternalUnexpected rather than as
// a user error. Guessing the other way would blame a customer for our bug.
func CodeOf(err error) Code {
	if err == nil {
		return ""
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return CodeInternalUnexpected
}

// ClassOf returns the class of the outermost coded error in the chain.
func ClassOf(err error) Class { return CodeOf(err).Class() }

// HasCode reports whether the chain carries this exact code. Prefer ClassOf
// when the caller only needs to know whose fault it is: it keeps working as
// new codes appear.
func HasCode(err error, code Code) bool { return CodeOf(err) == code }

// PositionOf returns the position of the outermost coded error, if it has
// one.
func PositionOf(err error) (Position, bool) {
	var e *Error
	if errors.As(err, &e) && e.Pos != nil {
		return *e.Pos, true
	}
	return Position{}, false
}
