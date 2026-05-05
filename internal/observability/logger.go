package observability

import (
	"log/slog"
	"os"
)

// Init configures slog as the process-wide default logger.
// Text format is used by default (readable in local console).
// Set LOG_FORMAT=json to switch to JSON (recommended in production).
func Init() {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}

	var handler slog.Handler
	if os.Getenv("LOG_FORMAT") == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	slog.SetDefault(slog.New(handler))
}
