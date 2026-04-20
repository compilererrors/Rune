package output

// EventSummary matches RuneCore EventSummary for agent JSON.
type EventSummary struct {
	Type              string  `json:"type"`
	Reason            string  `json:"reason"`
	ObjectName        string  `json:"objectName"`
	Message           string  `json:"message"`
	LastTimestamp     *string `json:"lastTimestamp,omitempty"`
	InvolvedKind      *string `json:"involvedKind,omitempty"`
	InvolvedNamespace *string `json:"involvedNamespace,omitempty"`
}
