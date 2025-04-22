package handlers

import (
	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/core"
)

/*
type StructuredBatchHandler struct {
	mu     sync.Mutex
	batch  []jsonRow
	pool   memory.Allocator
	schema *arrow.Schema
}

// jsonRow holds raw JSON messages
type jsonRow []byte

func NewStructuredBatchHandler() *StructuredBatchHandler {
	return &StructuredBatchHandler{
		pool:  memory.NewGoAllocator(),
		batch: make([]jsonRow, 0),
	}
}

func (h *StructuredBatchHandler) Init() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.batch = nil
}

func (h *StructuredBatchHandler) Write(msg string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.batch = append(h.batch, []byte(msg))
	return nil
}

func (h *StructuredBatchHandler) Invoke() (any, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.batch) == 0 {
		return nil, fmt.Errorf("no data to invoke")
	}

	// Combine all []byte JSON into one io.Reader
	var joined []byte
	for i, row := range h.batch {
		joined = append(joined, row...)
		if i < len(h.batch)-1 {
			joined = append(joined, '\n')
		}
	}
	return nil, nil

	/*
				reader := bytes.NewReader(joined)
			// Use arrjson.NewReader to decode into Arrow
			aj, err := arrjson.NewReader(reader, arrjson.WithAllocator(h.pool))
			if err != nil {
				return nil, fmt.Errorf("arrow json reader error: %w", err)
			}
			defer aj.Release()

			records := make([]array.Record, 0)
			for aj.Next() {
				rec := aj.Record()
				rec.Retain()
				records = append(records, rec)
			}

			if err := aj.Err(); err != nil {
				return nil, fmt.Errorf("arrow record iteration error: %w", err)
			}

			// Join records into one
			if len(records) == 0 {
				return nil, fmt.Errorf("no valid arrow records from json")
			}

			combined := array.NewRecordReader(records[0].Schema(), records)
			defer func() {
				for _, rec := range records {
					rec.Release()
				}
			}()

			result := combined.Record()
			result.Retain()

			// Send to DuckDB (example — not mandatory here)
			err = h.sendToDuckDB(result)
			if err != nil {
				return nil, fmt.Errorf("duckdb insert failed: %w", err)
			}

		return result, nil
}
*/

func New(c config.Handler) (core.Handler, error) {
	return nil, nil
}
