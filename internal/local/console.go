package local

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type ConsoleSink struct {
	mu      sync.Mutex
	buf     []any
	encoder *json.Encoder
	out     *os.File
}

func NewConsoleSink() *ConsoleSink {
	return &ConsoleSink{
		out:     os.Stdout,
		encoder: json.NewEncoder(os.Stdout),
	}
}

func (s *ConsoleSink) WriteTable(data any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Retain to prevent premature release
	s.buf = append(s.buf, data)
	return nil
}

func (s *ConsoleSink) Batch() any {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.buf
}

func (s *ConsoleSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.buf) == 0 {
		return nil
	}

	for _, rec := range s.buf {
		fmt.Println(rec)
	}
	s.buf = nil
	return nil
}
