package list

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rune/rune-k8s-agent/internal/kube"
	"github.com/rune/rune-k8s-agent/internal/output"
)

// Secrets lists core/v1 secrets in a namespace.
func Secrets(ctx context.Context, contextName, namespace string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.CoreV1().Secrets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, secret := range list.Items {
		if secret.Name == "" {
			continue
		}
		ns := secret.Namespace
		if ns == "" {
			ns = namespace
		}
		nsCopy := ns
		kind := secret.Type
		if kind == "" {
			kind = corev1.SecretTypeOpaque
		}
		out = append(out, output.Summary{
			Kind:          "secret",
			Name:          secret.Name,
			Namespace:     &nsCopy,
			PrimaryText:   string(kind),
			SecondaryText: fmt.Sprintf("%d values", len(secret.Data)),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// PersistentVolumeClaims lists core/v1 persistent volume claims in a namespace.
func PersistentVolumeClaims(ctx context.Context, contextName, namespace string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, pvc := range list.Items {
		if pvc.Name == "" {
			continue
		}
		ns := pvc.Namespace
		if ns == "" {
			ns = namespace
		}
		nsCopy := ns
		phase := string(pvc.Status.Phase)
		if phase == "" {
			phase = "Unknown"
		}
		size := "—"
		if q, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok && !q.IsZero() {
			size = q.String()
		} else if q, ok := pvc.Status.Capacity[corev1.ResourceStorage]; ok && !q.IsZero() {
			size = q.String()
		}
		out = append(out, output.Summary{
			Kind:          "persistentVolumeClaim",
			Name:          pvc.Name,
			Namespace:     &nsCopy,
			PrimaryText:   phase,
			SecondaryText: size,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// PersistentVolumes lists cluster-scoped core/v1 persistent volumes.
func PersistentVolumes(ctx context.Context, contextName string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, pv := range list.Items {
		if pv.Name == "" {
			continue
		}
		phase := string(pv.Status.Phase)
		if phase == "" {
			phase = "Unknown"
		}
		size := "—"
		if q, ok := pv.Spec.Capacity[corev1.ResourceStorage]; ok && !q.IsZero() {
			size = q.String()
		}
		out = append(out, output.Summary{
			Kind:          "persistentVolume",
			Name:          pv.Name,
			Namespace:     nil,
			PrimaryText:   phase,
			SecondaryText: size,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// StorageClasses lists cluster-scoped storage classes.
func StorageClasses(ctx context.Context, contextName string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, sc := range list.Items {
		if sc.Name == "" {
			continue
		}
		secondary := "StorageClass"
		if sc.Annotations["storageclass.kubernetes.io/is-default-class"] == "true" ||
			sc.Annotations["storageclass.beta.kubernetes.io/is-default-class"] == "true" {
			secondary = "Default"
		}
		provisioner := sc.Provisioner
		if provisioner == "" {
			provisioner = "—"
		}
		out = append(out, output.Summary{
			Kind:          "storageClass",
			Name:          sc.Name,
			Namespace:     nil,
			PrimaryText:   provisioner,
			SecondaryText: secondary,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// HorizontalPodAutoscalers lists autoscaling/v2 HPAs in a namespace.
func HorizontalPodAutoscalers(ctx context.Context, contextName, namespace string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.AutoscalingV2().HorizontalPodAutoscalers(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, hpa := range list.Items {
		if hpa.Name == "" {
			continue
		}
		ns := hpa.Namespace
		if ns == "" {
			ns = namespace
		}
		nsCopy := ns
		min := "—"
		if hpa.Spec.MinReplicas != nil {
			min = fmt.Sprintf("%d", *hpa.Spec.MinReplicas)
		}
		max := fmt.Sprintf("%d", hpa.Spec.MaxReplicas)
		current := fmt.Sprintf("%d", hpa.Status.CurrentReplicas)
		targetKind := hpa.Spec.ScaleTargetRef.Kind
		if targetKind == "" {
			targetKind = "?"
		}
		targetName := hpa.Spec.ScaleTargetRef.Name
		if targetName == "" {
			targetName = "?"
		}
		out = append(out, output.Summary{
			Kind:          "horizontalPodAutoscaler",
			Name:          hpa.Name,
			Namespace:     &nsCopy,
			PrimaryText:   fmt.Sprintf("%s–%s replicas (current %s)", min, max, current),
			SecondaryText: fmt.Sprintf("%s/%s", targetKind, targetName),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// NetworkPolicies lists networking.k8s.io/v1 network policies in a namespace.
func NetworkPolicies(ctx context.Context, contextName, namespace string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.NetworkingV1().NetworkPolicies(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, policy := range list.Items {
		if policy.Name == "" {
			continue
		}
		ns := policy.Namespace
		if ns == "" {
			ns = namespace
		}
		nsCopy := ns
		types := make([]string, 0, len(policy.Spec.PolicyTypes))
		for _, t := range policy.Spec.PolicyTypes {
			types = append(types, string(t))
		}
		primary := "—"
		if len(types) > 0 {
			primary = strings.Join(types, ", ")
		}
		out = append(out, output.Summary{
			Kind:          "networkPolicy",
			Name:          policy.Name,
			Namespace:     &nsCopy,
			PrimaryText:   primary,
			SecondaryText: "NetworkPolicy",
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
