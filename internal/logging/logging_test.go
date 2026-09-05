package logging

import (
	"testing"

	"github.com/zeebo/assert"
	"go.uber.org/zap/zapcore"
)

// sqlflow.settings defaults SQLFLOW_LOG_LEVEL to INFO, which is quieter than
// zap's development logger.
func TestSinkRetry_NewDefaultsToInfo(t *testing.T) {
	t.Setenv("SQLFLOW_LOG_LEVEL", "")

	l, err := New()
	assert.NoError(t, err)
	assert.That(t, l.Core().Enabled(zapcore.InfoLevel))
	assert.That(t, !l.Core().Enabled(zapcore.DebugLevel))
}

func TestSinkRetry_NewHonorsEnvVar(t *testing.T) {
	tests := []struct {
		level string
		want  zapcore.Level
	}{
		{level: "debug", want: zapcore.DebugLevel},
		{level: "DEBUG", want: zapcore.DebugLevel},
		{level: "info", want: zapcore.InfoLevel},
		{level: " warn ", want: zapcore.WarnLevel},
		// Python's level names, which zap does not know.
		{level: "WARNING", want: zapcore.WarnLevel},
		{level: "CRITICAL", want: zapcore.FatalLevel},
		{level: "error", want: zapcore.ErrorLevel},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			t.Setenv("SQLFLOW_LOG_LEVEL", tt.level)

			level, err := Level()
			assert.NoError(t, err)
			assert.Equal(t, tt.want, level)

			l, err := New()
			assert.NoError(t, err)
			assert.That(t, l.Core().Enabled(tt.want))
			if tt.want > zapcore.DebugLevel {
				assert.That(t, !l.Core().Enabled(tt.want-1))
			}
		})
	}
}

// Python's logging.basicConfig raises on an unknown level; the returned logger
// still works so callers that only log the failure are not left without one.
func TestSinkRetry_NewReportsUnknownLevel(t *testing.T) {
	t.Setenv("SQLFLOW_LOG_LEVEL", "chatty")

	_, err := Level()
	assert.Error(t, err)

	l, err := New()
	assert.Error(t, err)
	assert.That(t, l != nil)
	assert.That(t, l.Core().Enabled(zapcore.InfoLevel))
}
