package output

// DeploymentSummary matches RuneCore DeploymentSummary (Codable) for agent JSON.
type DeploymentSummary struct {
	Name            string            `json:"name"`
	Namespace       string            `json:"namespace"`
	ReadyReplicas   int               `json:"readyReplicas"`
	DesiredReplicas int               `json:"desiredReplicas"`
	Selector        map[string]string `json:"selector,omitempty"`
}
