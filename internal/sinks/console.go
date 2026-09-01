package sinks

import (
	"bufio"
	"io"
	"os"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// ConsoleSink writes each result row to stdout as a JSON object.
type ConsoleSink struct {
	mu    sync.Mutex
	out   *bufio.Writer
	batch arrow.Table
}

func NewConsoleSink() *ConsoleSink {
	return NewConsoleSinkTo(os.Stdout)
}

func NewConsoleSinkTo(w io.Writer) *ConsoleSink {
	return &ConsoleSink{out: bufio.NewWriter(w)}
}

func (s *ConsoleSink) WriteTable(batch arrow.Table) error {
	rows, err := tableRowsAsJSON(batch)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.batch = batch
	for _, row := range rows {
		if _, err := s.out.Write(row); err != nil {
			return err
		}
		if err := s.out.WriteByte('\n'); err != nil {
			return err
		}
	}
	return nil
}

func (s *ConsoleSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.out.Flush()
}

func (s *ConsoleSink) Batch() (arrow.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batch, nil
}
