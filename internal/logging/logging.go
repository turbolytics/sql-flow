// Package logging builds the logger every sqlflow command uses, taking its
// level from SQLFLOW_LOG_LEVEL exactly as sqlflow.settings does.
package logging

import (
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// EnvVar is the environment variable the Python engine reads in
// sqlflow/settings.py.
const EnvVar = "SQLFLOW_LOG_LEVEL"

// DefaultLevel matches settings.LOG_LEVEL's default. Notably it is not zap's
// development default of DEBUG.
const DefaultLevel = zapcore.InfoLevel

// pythonLevels are the level names the standard library's logging module
// accepts that zapcore.ParseLevel does not.
var pythonLevels = map[string]zapcore.Level{
	"warning":  zapcore.WarnLevel,
	"critical": zapcore.FatalLevel,
	// logging.NOTSET on the root logger means "log everything".
	"notset": zapcore.DebugLevel,
}

// Level resolves SQLFLOW_LOG_LEVEL. An unset or empty value is the default;
// an unrecognized one is an error, as it is in Python.
func Level() (zapcore.Level, error) {
	name := strings.ToLower(strings.TrimSpace(os.Getenv(EnvVar)))
	if name == "" {
		return DefaultLevel, nil
	}

	if level, ok := pythonLevels[name]; ok {
		return level, nil
	}

	level, err := zapcore.ParseLevel(name)
	if err != nil {
		return DefaultLevel, fmt.Errorf("%s: unknown level %q", EnvVar, name)
	}
	return level, nil
}

// New builds the console logger. On an unparseable level it returns both the
// error and a logger at the default level, so a caller with nowhere to report
// the failure still has somewhere to log it.
func New() (*zap.Logger, error) {
	level, levelErr := Level()

	conf := zap.NewDevelopmentConfig()
	conf.Level = zap.NewAtomicLevelAt(level)

	logger, err := conf.Build()
	if err != nil {
		return zap.NewNop(), err
	}
	return logger, levelErr
}
