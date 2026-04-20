package ops

import (
	"context"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rune/rune-k8s-agent/internal/kube"
	"github.com/rune/rune-k8s-agent/internal/output"
)

func ServiceSelector(ctx context.Context, contextName, namespace, serviceName string) (map[string]string, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	svc, err := clientset.CoreV1().Services(namespace).Get(ctx, serviceName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	selector := map[string]string{}
	for key, value := range svc.Spec.Selector {
		if key == "" || value == "" {
			continue
		}
		selector[key] = value
	}
	return selector, nil
}

func DeploymentSelector(ctx context.Context, contextName, namespace, deploymentName string) (map[string]string, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	deploy, err := clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	selector := map[string]string{}
	if deploy.Spec.Selector == nil {
		return selector, nil
	}
	for key, value := range deploy.Spec.Selector.MatchLabels {
		if key == "" || value == "" {
			continue
		}
		selector[key] = value
	}
	return selector, nil
}

func PodsBySelector(ctx context.Context, contextName, namespace, selector string) ([]output.PodSummary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return nil, err
	}
	out := make([]output.PodSummary, 0, len(list.Items))
	for _, pod := range list.Items {
		if pod.Name == "" {
			continue
		}
		status := string(pod.Status.Phase)
		if status == "" {
			status = "Unknown"
		}
		out = append(out, output.PodSummary{
			Name:      pod.Name,
			Namespace: namespace,
			Status:    status,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
