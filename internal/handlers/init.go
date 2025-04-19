package handlers

import (
	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/core"
)

type StructuredBatch struct {
	sql  string
	rows []any
}

func New(c config.Handler) (core.Handler, error) {
	return nil, nil
}
