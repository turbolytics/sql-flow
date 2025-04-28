package local

import (
	"encoding/json"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"os"
	"sync"
)

type ConsoleSink struct {
	mu      sync.Mutex
	buf     []arrow.Table
	encoder *json.Encoder
	out     *os.File
}

func NewConsoleSink() *ConsoleSink {
	return &ConsoleSink{
		out:     os.Stdout,
		encoder: json.NewEncoder(os.Stdout),
	}
}

func (s *ConsoleSink) WriteTable(batch arrow.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Retain to prevent premature release
	s.buf = append(s.buf, batch)
	return nil
}

func (s *ConsoleSink) Batch() (arrow.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return nil, nil
}

func (s *ConsoleSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.buf) == 0 {
		return nil
	}

	for _, table := range s.buf {
		tr := array.NewTableReader(table, 0)
		for tr.Next() {
			rec := tr.Record()
			if err := s.encoder.Encode(rec); err != nil {
				return err
			}
		}
	}
	s.buf = nil
	return nil
}
