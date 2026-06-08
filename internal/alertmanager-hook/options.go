package alertmanagerhook

import "log/slog"

type Options struct {
	TargetAgent       string
	AllowedSeverities []string
	IncludeResolved   bool
	Logger            *slog.Logger
}
