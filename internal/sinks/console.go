package sinks

import (
	"context"
	"io"
	"os"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// ConsoleSink writes each result row to stdout as a JSON object.
//
// The rows are held here rather than in a bufio.Writer. bufio latches its
// first error permanently: once a write failed, every later Flush returned
// that same stale error and the bytes were gone, so the sink never recovered
// even after the condition cleared. stdout is not immune to that -- redirect
// it to a full disk or a pipe whose reader exits and the write fails -- and a
// sink that cannot recover turns a transient failure into a dead pipeline.
type ConsoleSink struct {
	mu      sync.Mutex
	out     io.Writer
	pending []byte
	batch   arrow.Table
}

func NewConsoleSink() *ConsoleSink {
	return NewConsoleSinkTo(os.Stdout)
}

func NewConsoleSinkTo(w io.Writer) *ConsoleSink {
	return &ConsoleSink{out: w}
}

// WriteTable buffers the rows. Nothing reaches the writer until Flush, so the
// pipeline never commits offsets for a row that only got as far as this
// process.
func (s *ConsoleSink) WriteTable(ctx context.Context, batch arrow.Table) error {
	rows, err := tableRowsAsJSON(batch)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.batch = batch
	for _, row := range rows {
		s.pending = append(s.pending, row...)
		s.pending = append(s.pending, '\n')
	}
	return nil
}

// Flush writes the buffered rows, and keeps whatever it could not write.
//
// A short write leaves exactly the unwritten tail pending, so a retry sends
// the remainder rather than the whole batch again: the reader must not see a
// row twice because the tail of the batch failed.
func (s *ConsoleSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.pending) == 0 {
		return nil
	}

	n, err := s.out.Write(s.pending)
	if n > 0 {
		s.pending = s.pending[n:]
	}
	if err != nil {
		return err
	}
	if len(s.pending) != 0 {
		// io.Writer may return a short write with no error; the contract says
		// that is still a failure, and the remainder stays pending.
		return io.ErrShortWrite
	}
	return nil
}

func (s *ConsoleSink) Batch() (arrow.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batch, nil
}
