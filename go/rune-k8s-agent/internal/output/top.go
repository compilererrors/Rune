package output

// NodeTopPercent is average node usage percentages (CPU / memory) for the selected context.
type NodeTopPercent struct {
	CPUPercent    *int `json:"cpuPercent,omitempty"`
	MemoryPercent *int `json:"memoryPercent,omitempty"`
}

// PodTopUsage is one pod metrics row.
type PodTopUsage struct {
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
	CPU       string `json:"cpu"`
	Memory    string `json:"memory"`
}
