package output

type UnifiedLogs struct {
	PodNames   []string `json:"podNames"`
	MergedText string   `json:"mergedText"`
}
