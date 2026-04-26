package list

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rune/rune-k8s-agent/internal/kube"
	"github.com/rune/rune-k8s-agent/internal/output"
)

// Services lists core/v1 services in a namespace.
func Services(ctx context.Context, contextName, namespace string) ([]output.ServiceSummary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.ServiceSummary, 0, len(list.Items))
	for _, svc := range list.Items {
		name := svc.Name
		if name == "" {
			continue
		}
		ns := svc.Namespace
		if ns == "" {
			ns = namespace
		}
		typ := string(svc.Spec.Type)
		if typ == "" {
			typ = "ClusterIP"
		}
		clusterIP := svc.Spec.ClusterIP
		if clusterIP == "" {
			clusterIP = "None"
		}
		out = append(out, output.ServiceSummary{
			Name:      name,
			Namespace: ns,
			Type:      typ,
			ClusterIP: clusterIP,
			Selector:  svc.Spec.Selector,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Ingresses lists networking.k8s.io/v1 ingresses in a namespace.
func Ingresses(ctx context.Context, contextName, namespace string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.NetworkingV1().Ingresses(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, ing := range list.Items {
		name := ing.Name
		if name == "" {
			continue
		}
		ns := ing.Namespace
		if ns == "" {
			ns = namespace
		}
		primary := "—"
		if len(ing.Spec.Rules) > 0 && ing.Spec.Rules[0].Host != "" {
			primary = ing.Spec.Rules[0].Host
		}
		secondary := "Ingress"
		if len(ing.Status.LoadBalancer.Ingress) > 0 {
			addr := ing.Status.LoadBalancer.Ingress[0]
			if addr.Hostname != "" {
				secondary = addr.Hostname
			} else if addr.IP != "" {
				secondary = addr.IP
			}
		}
		nsCopy := ns
		out = append(out, output.Summary{
			Kind:          "ingress",
			Name:          name,
			Namespace:     &nsCopy,
			PrimaryText:   primary,
			SecondaryText: secondary,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ConfigMaps lists core/v1 configmaps in a namespace.
func ConfigMaps(ctx context.Context, contextName, namespace string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.CoreV1().ConfigMaps(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, cm := range list.Items {
		name := cm.Name
		if name == "" {
			continue
		}
		ns := cm.Namespace
		if ns == "" {
			ns = namespace
		}
		entries := len(cm.Data) + len(cm.BinaryData)
		nsCopy := ns
		out = append(out, output.Summary{
			Kind:          "configMap",
			Name:          name,
			Namespace:     &nsCopy,
			PrimaryText:   fmt.Sprintf("%d entries", entries),
			SecondaryText: "ConfigMap",
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Nodes lists core/v1 nodes (cluster-scoped).
func Nodes(ctx context.Context, contextName string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, node := range list.Items {
		name := node.Name
		if name == "" {
			continue
		}
		out = append(out, output.Summary{
			Kind:          "node",
			Name:          name,
			Namespace:     nil,
			PrimaryText:   nodeReadyText(node.Status),
			SecondaryText: nodeVersionText(node.Status),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Events lists core/v1 events in a namespace.
func Events(ctx context.Context, contextName, namespace string) ([]output.EventSummary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.EventSummary, 0, len(list.Items))
	for _, event := range list.Items {
		typ := event.Type
		if typ == "" {
			typ = "Normal"
		}
		reason := event.Reason
		objectName := event.InvolvedObject.Name
		msg := event.Message
		var lastTimestamp *string
		if ts := eventLastTimestamp(event); ts != "" {
			v := ts
			lastTimestamp = &v
		}
		var involvedKind *string
		if event.InvolvedObject.Kind != "" {
			v := event.InvolvedObject.Kind
			involvedKind = &v
		}
		var involvedNamespace *string
		if event.InvolvedObject.Namespace != "" {
			v := event.InvolvedObject.Namespace
			involvedNamespace = &v
		}
		out = append(out, output.EventSummary{
			Type:              typ,
			Reason:            reason,
			ObjectName:        objectName,
			Message:           msg,
			LastTimestamp:     lastTimestamp,
			InvolvedKind:      involvedKind,
			InvolvedNamespace: involvedNamespace,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		left := out[i].LastTimestamp
		right := out[j].LastTimestamp
		if left != nil && right != nil && *left != *right {
			return *left > *right
		}
		if out[i].Reason != out[j].Reason {
			return out[i].Reason < out[j].Reason
		}
		return out[i].ObjectName < out[j].ObjectName
	})
	return out, nil
}

func nodeReadyText(status corev1.NodeStatus) string {
	for _, condition := range status.Conditions {
		if condition.Type == corev1.NodeReady {
			if condition.Status == corev1.ConditionTrue {
				return "Ready"
			}
			return "NotReady"
		}
	}
	return "Unknown"
}

func nodeVersionText(status corev1.NodeStatus) string {
	if status.NodeInfo.KubeletVersion != "" {
		return status.NodeInfo.KubeletVersion
	}
	return "Node"
}

func eventLastTimestamp(event corev1.Event) string {
	if !event.LastTimestamp.IsZero() {
		return event.LastTimestamp.UTC().Format(time.RFC3339)
	}
	if !event.EventTime.IsZero() {
		return event.EventTime.UTC().Format(time.RFC3339)
	}
	if !event.FirstTimestamp.IsZero() {
		return event.FirstTimestamp.UTC().Format(time.RFC3339)
	}
	return ""
}
