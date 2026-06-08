package alertmanagerhook

import (
	"fmt"
	"sort"
	"strings"
)

func FormatAlertMessage(alert Alert) string {
	labelPairs := make([]string, 0, len(alert.Labels))
	for k, v := range alert.Labels {
		labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", k, v))
	}
	sort.Strings(labelPairs)

	return strings.Join([]string{
		fmt.Sprintf("Alert: %s", alert.Labels["alertname"]),
		fmt.Sprintf("Severity: %s", alert.Labels["severity"]),
		fmt.Sprintf("Status: %s", alert.Status),
		fmt.Sprintf("Namespace: %s", alert.Labels["namespace"]),
		fmt.Sprintf("Cluster: %s", alert.Labels["cluster"]),
		fmt.Sprintf("Started: %s", alert.StartsAt.Format("2006-01-02T15:04:05Z07:00")),
		fmt.Sprintf("Summary: %s", alert.Annotations["summary"]),
		fmt.Sprintf("Description: %s", alert.Annotations["description"]),
		fmt.Sprintf("Labels: %s", strings.Join(labelPairs, ", ")),
	}, "\n")
}
