// Package output defines JSON written to stdout for the Swift/Rune side (ClusterResourceSummary).
package output

// Summary is one row for Rune’s workload lists (mirrors RuneCore ClusterResourceSummary fields).
type Summary struct {
	Kind          string  `json:"kind"`
	Name          string  `json:"name"`
	Namespace     *string `json:"namespace,omitempty"`
	PrimaryText   string  `json:"primaryText"`
	SecondaryText string  `json:"secondaryText"`
}
