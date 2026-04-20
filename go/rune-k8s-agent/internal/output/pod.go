package output

// PodSummary matches the subset of RuneCore PodSummary that the app needs from agent reads.
type PodSummary struct {
	Name               string  `json:"name"`
	Namespace          string  `json:"namespace"`
	Status             string  `json:"status"`
	TotalRestarts      int     `json:"totalRestarts"`
	CreationTimestamp  string  `json:"creationTimestamp,omitempty"`
	PodIP              *string `json:"podIP,omitempty"`
	HostIP             *string `json:"hostIP,omitempty"`
	NodeName           *string `json:"nodeName,omitempty"`
	QoSClass           *string `json:"qosClass,omitempty"`
	ContainersReady    *string `json:"containersReady,omitempty"`
	ContainerNamesLine *string `json:"containerNamesLine,omitempty"`
}
