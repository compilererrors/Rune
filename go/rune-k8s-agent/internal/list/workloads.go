// Package list implements read-only list operations via client-go.
// API layout matches Kubernetes REST paths used in Rune’s Swiftkube-inspired KubernetesRESTPath
// (e.g. GET /apis/apps/v1/namespaces/{ns}/statefulsets, /apis/batch/v1/namespaces/{ns}/jobs).
package list

import (
	"context"
	"fmt"
	"sort"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rune/rune-k8s-agent/internal/kube"
	"github.com/rune/rune-k8s-agent/internal/output"
)

// Jobs lists batch/v1 Jobs in a namespace (REST: .../batch/v1/namespaces/{ns}/jobs).
func Jobs(ctx context.Context, contextName, namespace string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, job := range list.Items {
		name := job.Name
		if name == "" {
			continue
		}
		ns := job.Namespace
		label := jobStatusLabel(job.Status)
		nsPtr := &ns
		if ns == "" {
			nsPtr = nil
		}
		out = append(out, output.Summary{
			Kind:          "job",
			Name:          name,
			Namespace:     nsPtr,
			PrimaryText:   label,
			SecondaryText: "Job",
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// CronJobs lists batch/v1 CronJobs (REST: .../batch/v1/namespaces/{ns}/cronjobs).
func CronJobs(ctx context.Context, contextName, namespace string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.BatchV1().CronJobs(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, cron := range list.Items {
		name := cron.Name
		if name == "" {
			continue
		}
		ns := cron.Namespace
		schedule := "—"
		if cron.Spec.Schedule != "" {
			schedule = cron.Spec.Schedule
		}
		suspended := cron.Spec.Suspend != nil && *cron.Spec.Suspend
		secondary := "Active"
		if suspended {
			secondary = "Suspended"
		}
		nsPtr := &ns
		if ns == "" {
			nsPtr = nil
		}
		out = append(out, output.Summary{
			Kind:          "cronJob",
			Name:          name,
			Namespace:     nsPtr,
			PrimaryText:   schedule,
			SecondaryText: secondary,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// DaemonSets lists apps/v1 DaemonSets (REST: .../apps/v1/namespaces/{ns}/daemonsets).
func DaemonSets(ctx context.Context, contextName, namespace string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.AppsV1().DaemonSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, ds := range list.Items {
		name := ds.Name
		if name == "" {
			continue
		}
		ns := ds.Namespace
		desired := ds.Status.DesiredNumberScheduled
		ready := ds.Status.NumberReady
		nsPtr := &ns
		if ns == "" {
			nsPtr = nil
		}
		out = append(out, output.Summary{
			Kind:          "daemonSet",
			Name:          name,
			Namespace:     nsPtr,
			PrimaryText:   fmt.Sprintf("%d/%d", ready, desired),
			SecondaryText: "DaemonSet",
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// StatefulSets lists apps/v1 StatefulSets (REST: .../apps/v1/namespaces/{ns}/statefulsets).
func StatefulSets(ctx context.Context, contextName, namespace string) ([]output.Summary, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	list, err := clientset.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]output.Summary, 0, len(list.Items))
	for _, sts := range list.Items {
		name := sts.Name
		if name == "" {
			continue
		}
		ns := sts.Namespace
		desired := int32(1)
		if sts.Spec.Replicas != nil {
			desired = *sts.Spec.Replicas
		}
		ready := sts.Status.ReadyReplicas
		nsPtr := &ns
		if ns == "" {
			nsPtr = nil
		}
		out = append(out, output.Summary{
			Kind:          "statefulSet",
			Name:          name,
			Namespace:     nsPtr,
			PrimaryText:   fmt.Sprintf("%d/%d", ready, desired),
			SecondaryText: "StatefulSet",
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func jobStatusLabel(status batchv1.JobStatus) string {
	failed := status.Failed
	succeeded := status.Succeeded
	active := status.Active
	if failed > 0 {
		return fmt.Sprintf("Failed (%d)", failed)
	}
	if succeeded > 0 {
		return fmt.Sprintf("Complete (%d)", succeeded)
	}
	if active > 0 {
		return fmt.Sprintf("Running (%d)", active)
	}
	return "Pending"
}
