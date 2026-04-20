package list

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rune/rune-k8s-agent/internal/kube"
	"github.com/rune/rune-k8s-agent/internal/output"
)

// Pods lists pods in a namespace with the subset Rune overview/workloads need.
func Pods(ctx context.Context, contextName, namespace string) ([]output.PodSummary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.PodSummary, 0, len(list.Items))
	for _, pod := range list.Items {
		name := pod.Name
		if name == "" {
			continue
		}
		ns := pod.Namespace
		if ns == "" {
			ns = namespace
		}
		status := string(pod.Status.Phase)
		if status == "" {
			status = "Unknown"
		}
		totalRestarts := totalPodRestarts(pod.Status)
		creation := ""
		if !pod.CreationTimestamp.IsZero() {
			creation = pod.CreationTimestamp.Time.UTC().Format(time.RFC3339)
		}

		var podIP *string
		if pod.Status.PodIP != "" {
			v := pod.Status.PodIP
			podIP = &v
		}
		var hostIP *string
		if pod.Status.HostIP != "" {
			v := pod.Status.HostIP
			hostIP = &v
		}
		var nodeName *string
		if pod.Spec.NodeName != "" {
			v := pod.Spec.NodeName
			nodeName = &v
		}
		var qosClass *string
		if pod.Status.QOSClass != "" {
			v := string(pod.Status.QOSClass)
			qosClass = &v
		}
		containersReady := podContainersReady(pod.Status)
		containerNamesLine := podContainerNamesLine(pod.Spec)

		out = append(out, output.PodSummary{
			Name:               name,
			Namespace:          ns,
			Status:             status,
			TotalRestarts:      totalRestarts,
			CreationTimestamp:  creation,
			PodIP:              podIP,
			HostIP:             hostIP,
			NodeName:           nodeName,
			QoSClass:           qosClass,
			ContainersReady:    containersReady,
			ContainerNamesLine: containerNamesLine,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func totalPodRestarts(status corev1.PodStatus) int {
	total := 0
	for _, s := range status.InitContainerStatuses {
		total += int(s.RestartCount)
	}
	for _, s := range status.ContainerStatuses {
		total += int(s.RestartCount)
	}
	for _, s := range status.EphemeralContainerStatuses {
		total += int(s.RestartCount)
	}
	return total
}

func podContainersReady(status corev1.PodStatus) *string {
	total := len(status.ContainerStatuses)
	if total == 0 {
		return nil
	}
	ready := 0
	for _, s := range status.ContainerStatuses {
		if s.Ready {
			ready++
		}
	}
	v := fmt.Sprintf("%d/%d", ready, total)
	return &v
}

func podContainerNamesLine(spec corev1.PodSpec) *string {
	if len(spec.Containers) == 0 {
		return nil
	}
	names := make([]string, 0, len(spec.Containers))
	for _, c := range spec.Containers {
		if c.Name == "" {
			continue
		}
		names = append(names, c.Name)
	}
	if len(names) == 0 {
		return nil
	}
	v := strings.Join(names, ", ")
	return &v
}
