package ops

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/rune/rune-k8s-agent/internal/kube"
	"github.com/rune/rune-k8s-agent/internal/output"
)

func TopNodesPercent(ctx context.Context, contextName string) (output.NodeTopPercent, error) {
	cfg, err := kube.NewRESTConfig(contextName)
	if err != nil {
		return output.NodeTopPercent{}, err
	}
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return output.NodeTopPercent{}, err
	}
	metricsClient, err := metricsclientset.NewForConfig(cfg)
	if err != nil {
		return output.NodeTopPercent{}, err
	}

	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return output.NodeTopPercent{}, err
	}
	nodeCap := make(map[string]corev1.ResourceList, len(nodes.Items))
	for _, node := range nodes.Items {
		nodeCap[node.Name] = node.Status.Capacity
	}

	metrics, err := metricsClient.MetricsV1beta1().NodeMetricses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return output.NodeTopPercent{}, err
	}

	var cpuValues []int
	var memValues []int
	for _, row := range metrics.Items {
		capacity, ok := nodeCap[row.Name]
		if !ok {
			continue
		}
		usageCPU := row.Usage[corev1.ResourceCPU]
		usageMem := row.Usage[corev1.ResourceMemory]
		capCPU := capacity[corev1.ResourceCPU]
		capMem := capacity[corev1.ResourceMemory]

		if percent := percentMilli(usageCPU, capCPU); percent != nil {
			cpuValues = append(cpuValues, *percent)
		}
		if percent := percentValue(usageMem, capMem); percent != nil {
			memValues = append(memValues, *percent)
		}
	}

	var cpuPercent *int
	if len(cpuValues) > 0 {
		v := averageRounded(cpuValues)
		cpuPercent = &v
	}
	var memoryPercent *int
	if len(memValues) > 0 {
		v := averageRounded(memValues)
		memoryPercent = &v
	}
	return output.NodeTopPercent{
		CPUPercent:    cpuPercent,
		MemoryPercent: memoryPercent,
	}, nil
}

func TopPods(ctx context.Context, contextName, namespace string) ([]output.PodTopUsage, error) {
	cfg, err := kube.NewRESTConfig(contextName)
	if err != nil {
		return nil, err
	}
	metricsClient, err := metricsclientset.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	list, err := metricsClient.MetricsV1beta1().PodMetricses(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.PodTopUsage, 0, len(list.Items))
	for _, row := range list.Items {
		if row.Name == "" {
			continue
		}
		cpu := resource.Quantity{}
		mem := resource.Quantity{}
		for _, c := range row.Containers {
			if q, ok := c.Usage[corev1.ResourceCPU]; ok {
				cpu.Add(q)
			}
			if q, ok := c.Usage[corev1.ResourceMemory]; ok {
				mem.Add(q)
			}
		}
		out = append(out, output.PodTopUsage{
			Name:   row.Name,
			CPU:    formatPodCPU(cpu),
			Memory: formatPodMemory(mem),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func TopPodsAllNamespaces(ctx context.Context, contextName string) ([]output.PodTopUsage, error) {
	cfg, err := kube.NewRESTConfig(contextName)
	if err != nil {
		return nil, err
	}
	metricsClient, err := metricsclientset.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	list, err := metricsClient.MetricsV1beta1().PodMetricses(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.PodTopUsage, 0, len(list.Items))
	for _, row := range list.Items {
		if row.Name == "" {
			continue
		}
		cpu := resource.Quantity{}
		mem := resource.Quantity{}
		for _, c := range row.Containers {
			if q, ok := c.Usage[corev1.ResourceCPU]; ok {
				cpu.Add(q)
			}
			if q, ok := c.Usage[corev1.ResourceMemory]; ok {
				mem.Add(q)
			}
		}
		out = append(out, output.PodTopUsage{
			Namespace: row.Namespace,
			Name:      row.Name,
			CPU:       formatPodCPU(cpu),
			Memory:    formatPodMemory(mem),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func percentMilli(usage, capacity resource.Quantity) *int {
	capacityMilli := capacity.MilliValue()
	if capacityMilli <= 0 {
		return nil
	}
	v := int((usage.MilliValue() * 100) / capacityMilli)
	return &v
}

func percentValue(usage, capacity resource.Quantity) *int {
	capacityValue := capacity.Value()
	if capacityValue <= 0 {
		return nil
	}
	v := int((usage.Value() * 100) / capacityValue)
	return &v
}

func averageRounded(values []int) int {
	total := 0
	for _, v := range values {
		total += v
	}
	if len(values) == 0 {
		return 0
	}
	// Integer rounding to nearest.
	return (total + len(values)/2) / len(values)
}

func formatPodCPU(quantity resource.Quantity) string {
	return fmt.Sprintf("%dm", quantity.MilliValue())
}

func formatPodMemory(quantity resource.Quantity) string {
	return fmt.Sprintf("%dMi", quantity.Value()/(1024*1024))
}
