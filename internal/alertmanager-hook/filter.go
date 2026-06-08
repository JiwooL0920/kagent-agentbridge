package alertmanagerhook

import "slices"

// SeverityFilter returns true if the alert should be forwarded.
func SeverityFilter(alert Alert, allowedSeverities []string) bool {
	return slices.Contains(allowedSeverities, alert.Labels["severity"])
}
