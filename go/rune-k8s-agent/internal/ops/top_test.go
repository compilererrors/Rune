package ops

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

func TestFormatPodCPUUsesMillicores(t *testing.T) {
	tests := map[string]string{
		"0":      "0m",
		"49377n": "1m",
		"250m":   "250m",
		"1500m":  "1500m",
		"2":      "2000m",
	}

	for raw, want := range tests {
		got := formatPodCPU(resource.MustParse(raw))
		if got != want {
			t.Fatalf("formatPodCPU(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestFormatPodMemoryUsesMebibytes(t *testing.T) {
	tests := map[string]string{
		"0":      "0Mi",
		"356Ki":  "0Mi",
		"1536Ki": "1Mi",
		"128Mi":  "128Mi",
		"1Gi":    "1024Mi",
	}

	for raw, want := range tests {
		got := formatPodMemory(resource.MustParse(raw))
		if got != want {
			t.Fatalf("formatPodMemory(%q) = %q, want %q", raw, got, want)
		}
	}
}
