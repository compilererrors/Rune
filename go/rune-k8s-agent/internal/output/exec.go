package output

// ExecResult is the serialized result from pod exec operations.
type ExecResult struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int32  `json:"exitCode"`
}
