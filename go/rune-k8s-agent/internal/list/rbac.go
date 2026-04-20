package list

import (
	"context"
	"fmt"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rune/rune-k8s-agent/internal/kube"
	"github.com/rune/rune-k8s-agent/internal/output"
)

// Roles lists namespaced roles.
func Roles(ctx context.Context, contextName, namespace string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.RbacV1().Roles(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, role := range list.Items {
		if role.Name == "" {
			continue
		}
		ns := role.Namespace
		if ns == "" {
			ns = namespace
		}
		nsCopy := ns
		out = append(out, output.Summary{
			Kind:          "role",
			Name:          role.Name,
			Namespace:     &nsCopy,
			PrimaryText:   fmt.Sprintf("%d rules", len(role.Rules)),
			SecondaryText: "Namespaced role",
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// RoleBindings lists namespaced role bindings.
func RoleBindings(ctx context.Context, contextName, namespace string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.RbacV1().RoleBindings(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, binding := range list.Items {
		if binding.Name == "" {
			continue
		}
		ns := binding.Namespace
		if ns == "" {
			ns = namespace
		}
		nsCopy := ns
		ref := binding.RoleRef.Name
		if ref == "" {
			ref = "-"
		}
		out = append(out, output.Summary{
			Kind:          "roleBinding",
			Name:          binding.Name,
			Namespace:     &nsCopy,
			PrimaryText:   fmt.Sprintf("→ %s", ref),
			SecondaryText: fmt.Sprintf("%d subject(s)", len(binding.Subjects)),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ClusterRoles lists cluster-scoped roles.
func ClusterRoles(ctx context.Context, contextName string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.RbacV1().ClusterRoles().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, role := range list.Items {
		if role.Name == "" {
			continue
		}
		out = append(out, output.Summary{
			Kind:          "clusterRole",
			Name:          role.Name,
			Namespace:     nil,
			PrimaryText:   fmt.Sprintf("%d rules", len(role.Rules)),
			SecondaryText: "Cluster role",
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ClusterRoleBindings lists cluster-scoped role bindings.
func ClusterRoleBindings(ctx context.Context, contextName string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, binding := range list.Items {
		if binding.Name == "" {
			continue
		}
		ref := binding.RoleRef.Name
		if ref == "" {
			ref = "-"
		}
		out = append(out, output.Summary{
			Kind:          "clusterRoleBinding",
			Name:          binding.Name,
			Namespace:     nil,
			PrimaryText:   fmt.Sprintf("→ %s", ref),
			SecondaryText: fmt.Sprintf("%d subject(s)", len(binding.Subjects)),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
