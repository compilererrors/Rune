package list

import (
	"context"
	"fmt"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rune/rune-k8s-agent/internal/kube"
	"github.com/rune/rune-k8s-agent/internal/output"
)

// Deployments lists apps/v1 Deployments (REST: .../apps/v1/namespaces/{ns}/deployments).
func Deployments(ctx context.Context, contextName, namespace string) ([]output.DeploymentSummary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.DeploymentSummary, 0, len(list.Items))
	for _, d := range list.Items {
		name := d.Name
		if name == "" {
			continue
		}
		ns := d.Namespace
		if ns == "" {
			ns = namespace
		}
		desired := int32(0)
		if d.Spec.Replicas != nil {
			desired = *d.Spec.Replicas
		}
		ready := d.Status.ReadyReplicas
		out = append(out, output.DeploymentSummary{
			Name:            name,
			Namespace:       ns,
			ReadyReplicas:   int(ready),
			DesiredReplicas: int(desired),
			Selector:        d.Spec.Selector.MatchLabels,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ReplicaSets lists apps/v1 ReplicaSets (REST: .../apps/v1/namespaces/{ns}/replicasets).
func ReplicaSets(ctx context.Context, contextName, namespace string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, rs := range list.Items {
		name := rs.Name
		if name == "" {
			continue
		}
		ns := rs.Namespace
		if ns == "" {
			ns = namespace
		}
		ready := rs.Status.ReadyReplicas
		total := rs.Status.Replicas
		if total == 0 && rs.Spec.Replicas != nil {
			total = *rs.Spec.Replicas
		}
		nsPtr := &ns
		if ns == "" {
			nsPtr = nil
		}
		out = append(out, output.Summary{
			Kind:          "replicaSet",
			Name:          name,
			Namespace:     nsPtr,
			PrimaryText:   fmt.Sprintf("%d/%d ready", ready, total),
			SecondaryText: "ReplicaSet",
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
