package handlers

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.uber.org/zap"
)

// File names the Python handler stages under sql_results_cache_dir. They are
// fixed, so two pipelines sharing a cache dir would overwrite each other's
// batches — same as the Python engine.
const (
	diskBatchFileName = "consumer_batch.json"
	diskOutFileName   = "out.json"
)

// InferredDiskBatchHandler buffers a batch of JSON messages to a file and
// lets DuckDB infer the schema, rather than holding the batch in memory.
//
// It follows the Python handler step for step: the batch file is loaded with
// read_json_auto into a `batch` table, the user SQL is run through COPY into
// an output file, and that file is read back as the result. The `batch` table
// is dropped after every invoke, so the next batch re-creates it against
// whatever schema its own messages imply.
type InferredDiskBatchHandler struct {
	cacheDir  string
	batchFile string
	outFile   string

	conn     adbc.Connection
	f        *os.File
	w        *bufio.Writer
	logger   *zap.Logger
	sql      string
	numWrote int
}

func NewInferredDiskBatchHandler(
	conn adbc.Connection,
	sql string,
	cacheDir string,
	opts ...InferredDiskBatchHandlerOption,
) (*InferredDiskBatchHandler, error) {
	if strings.TrimSpace(sql) == "" {
		return nil, fmt.Errorf("inferred disk batch: sql is required")
	}

	// The Python handler assumes the cache dir exists and fails on open()
	// otherwise; creating it removes a first-run failure that the config
	// gives no way to avoid.
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("create sql results cache dir: %w", err)
	}

	h := &InferredDiskBatchHandler{
		cacheDir:  cacheDir,
		batchFile: filepath.Join(cacheDir, diskBatchFileName),
		outFile:   filepath.Join(cacheDir, diskOutFileName),
		conn:      conn,
		logger:    zap.NewNop(),
		sql:       sql,
	}

	for _, opt := range opts {
		opt(h)
	}

	return h, nil
}

type InferredDiskBatchHandlerOption func(*InferredDiskBatchHandler)

func InferredDiskBatchWithLogger(l *zap.Logger) InferredDiskBatchHandlerOption {
	return func(h *InferredDiskBatchHandler) {
		h.logger = l
	}
}

// Init truncates the batch file, so a batch never inherits rows from the
// previous one, and clears any `batch` table left behind on the connection.
func (h *InferredDiskBatchHandler) Init(ctx context.Context) error {
	if err := h.closeBatchFile(); err != nil {
		return err
	}

	f, err := os.OpenFile(h.batchFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open batch file: %w", err)
	}
	h.f = f
	h.w = bufio.NewWriter(f)
	h.numWrote = 0

	return h.dropDiskBatchTable(ctx)
}

func (h *InferredDiskBatchHandler) Write(r []byte) error {
	if h.w == nil {
		return fmt.Errorf("inferred disk batch: handler is not initialized")
	}
	// Validated here rather than at Invoke so malformed JSON surfaces as a
	// per-message write error, which is what the error policies key off. A
	// bad line reaching the batch file would fail the whole batch instead.
	if !json.Valid(r) {
		return fmt.Errorf("invalid json: %q", truncate(r, 64))
	}

	if _, err := h.w.Write(r); err != nil {
		return err
	}
	if err := h.w.WriteByte('\n'); err != nil {
		return err
	}
	h.numWrote++
	return nil
}

func (h *InferredDiskBatchHandler) Invoke(ctx context.Context) (arrow.Table, error) {
	if err := h.closeBatchFile(); err != nil {
		return nil, err
	}

	if h.numWrote == 0 {
		return nil, fmt.Errorf("no records to invoke")
	}

	// Dropped whatever the outcome, matching the Python finally block: a
	// batch table surviving a failure would break the next CREATE TABLE.
	defer func() {
		if err := h.dropDiskBatchTable(ctx); err != nil {
			h.logger.Error("failed to drop batch table", zap.Error(err))
		}
	}()

	if err := h.exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s AS SELECT * FROM read_json_auto('%s')",
		batchTableName, h.batchFile,
	)); err != nil {
		return nil, fmt.Errorf("load batch file: %w", err)
	}

	if err := h.exec(ctx, fmt.Sprintf("COPY (%s) TO '%s'", h.sql, h.outFile)); err != nil {
		return nil, fmt.Errorf("copy query results: %w", err)
	}

	return h.readOutFile(ctx)
}

// readOutFile reads back what COPY wrote. DuckDB does the reading here where
// the Python handler uses pyarrow, so that the JSON the engine wrote is the
// JSON the engine parses; Arrow Go has no schema-inferring JSON reader.
func (h *InferredDiskBatchHandler) readOutFile(ctx context.Context) (arrow.Table, error) {
	defer func() {
		if err := os.Remove(h.outFile); err != nil && !os.IsNotExist(err) {
			h.logger.Error("failed to remove results file", zap.Error(err))
		}
	}()

	// A query matching no rows leaves COPY's file empty, which read_json
	// cannot infer a schema from. That is a normal batch outcome, not a
	// failure, so it yields an empty table.
	info, err := os.Stat(h.outFile)
	if err != nil {
		return nil, fmt.Errorf("stat results file: %w", err)
	}
	if info.Size() == 0 {
		return array.NewTable(arrow.NewSchema(nil, nil), nil, 0), nil
	}

	stmt, err := h.conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(fmt.Sprintf("SELECT * FROM read_json_auto('%s')", h.outFile)); err != nil {
		return nil, fmt.Errorf("set results query: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("read results file: %w", err)
	}
	defer reader.Release()

	var records []arrow.Record
	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		records = append(records, rec)
	}
	if err := reader.Err(); err != nil {
		for _, rec := range records {
			rec.Release()
		}
		return nil, err
	}

	result := array.NewTableFromRecords(reader.Schema(), records)
	result.Retain()

	for _, rec := range records {
		rec.Release()
	}

	return result, nil
}

// Close removes the staged files. The batch file grows with the batch size,
// so leaving it behind after shutdown wastes disk until the next run.
func (h *InferredDiskBatchHandler) Close() error {
	err := h.closeBatchFile()

	for _, path := range []string{h.batchFile, h.outFile} {
		if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) && err == nil {
			err = rmErr
		}
	}
	return err
}

// closeBatchFile flushes and closes the current batch file, if one is open.
func (h *InferredDiskBatchHandler) closeBatchFile() error {
	if h.f == nil {
		return nil
	}

	err := h.w.Flush()
	if closeErr := h.f.Close(); err == nil {
		err = closeErr
	}
	h.f, h.w = nil, nil

	if err != nil {
		return fmt.Errorf("close batch file: %w", err)
	}
	return nil
}

func (h *InferredDiskBatchHandler) dropDiskBatchTable(ctx context.Context) error {
	return h.exec(ctx, "DROP TABLE IF EXISTS "+batchTableName)
}

func (h *InferredDiskBatchHandler) exec(ctx context.Context, sql string) error {
	stmt, err := h.conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return err
	}
	_, err = stmt.ExecuteUpdate(ctx)
	return err
}
