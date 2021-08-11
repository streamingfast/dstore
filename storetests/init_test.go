package storetests

import (
	"os"

	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

var traceEnabled = os.Getenv("TRACE") == "true"

func init() {
	if os.Getenv("DEBUG") == "true" || traceEnabled {
		logger, _ := zap.NewDevelopment()
		logging.Override(logger)
	}
}
