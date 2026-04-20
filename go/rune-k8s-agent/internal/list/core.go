package list

import (
	"context"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/rune/rune-k8s-agent/internal/kube"
)

// Contexts returns kubeconfig context names from the active config loading rules.
func Contexts() ([]string, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := loadingRules.Load()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(cfg.Contexts))
	for name := range cfg.Contexts {
		if name == "" {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// ContextNamespace returns the default namespace configured for a kubeconfig context.
func ContextNamespace(contextName string) (string, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := loadingRules.Load()
	if err != nil {
		return "", err
	}
	ctx, ok := cfg.Contexts[contextName]
	if !ok || ctx == nil {
		return "", nil
	}
	return ctx.Namespace, nil
}

// Namespaces lists namespace names for a context via client-go.
func Namespaces(fetchContext context.Context, contextName string) ([]string, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.CoreV1().Namespaces().List(fetchContext, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(list.Items))
	for _, ns := range list.Items {
		if ns.Name == "" {
			continue
		}
		out = append(out, ns.Name)
	}
	sort.Strings(out)
	return out, nil
}
