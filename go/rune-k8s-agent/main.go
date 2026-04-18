// rune-k8s-agent lists Kubernetes resources using client-go (official Kubernetes Go client).
// It reads kubeconfig from the KUBECONFIG environment variable (same as kubectl).
// On success it prints a JSON array to stdout; errors go to stderr.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

type resourceSummary struct {
	Kind          string  `json:"kind"`
	Name          string  `json:"name"`
	Namespace     *string `json:"namespace,omitempty"`
	PrimaryText   string  `json:"primaryText"`
	SecondaryText string  `json:"secondaryText"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: rune-k8s-agent list <jobs|cronjobs|daemonsets|statefulsets> --context NAME --namespace NS")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "list":
		if err := runList(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}

func runList(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("list requires a resource: jobs or cronjobs")
	}
	resource := args[0]
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	var contextName, namespace string
	fs.StringVar(&contextName, "context", "", "kubeconfig context name")
	fs.StringVar(&namespace, "namespace", "", "namespace")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if contextName == "" {
		return fmt.Errorf("--context is required")
	}
	if namespace == "" {
		return fmt.Errorf("--namespace is required")
	}
	switch resource {
	case "jobs":
		return listJobs(contextName, namespace)
	case "cronjobs":
		return listCronJobs(contextName, namespace)
	case "daemonsets":
		return listDaemonSets(contextName, namespace)
	case "statefulsets":
		return listStatefulSets(contextName, namespace)
	default:
		return fmt.Errorf("unknown resource %q (use jobs, cronjobs, daemonsets, or statefulsets)", resource)
	}
}

func kubeClientSet(contextName string) (*kubernetes.Clientset, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{CurrentContext: contextName}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)
	restConfig, err := cc.ClientConfig()
	if err != nil {
		return nil, err
	}
	restConfig.Timeout = 120 * time.Second
	return kubernetes.NewForConfig(restConfig)
}

func listJobs(contextName, namespace string) error {
	clientset, err := kubeClientSet(contextName)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	list, err := clientset.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	out := make([]resourceSummary, 0, len(list.Items))
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
		out = append(out, resourceSummary{
			Kind:          "job",
			Name:          name,
			Namespace:     nsPtr,
			PrimaryText:   label,
			SecondaryText: "Job",
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return json.NewEncoder(os.Stdout).Encode(out)
}

func listDaemonSets(contextName, namespace string) error {
	clientset, err := kubeClientSet(contextName)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	list, err := clientset.AppsV1().DaemonSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	out := make([]resourceSummary, 0, len(list.Items))
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
		out = append(out, resourceSummary{
			Kind:          "daemonSet",
			Name:          name,
			Namespace:     nsPtr,
			PrimaryText:   fmt.Sprintf("%d/%d", ready, desired),
			SecondaryText: "DaemonSet",
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return json.NewEncoder(os.Stdout).Encode(out)
}

func listStatefulSets(contextName, namespace string) error {
	clientset, err := kubeClientSet(contextName)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	list, err := clientset.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	out := make([]resourceSummary, 0, len(list.Items))
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
		out = append(out, resourceSummary{
			Kind:          "statefulSet",
			Name:          name,
			Namespace:     nsPtr,
			PrimaryText:   fmt.Sprintf("%d/%d", ready, desired),
			SecondaryText: "StatefulSet",
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return json.NewEncoder(os.Stdout).Encode(out)
}

func listCronJobs(contextName, namespace string) error {
	clientset, err := kubeClientSet(contextName)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	list, err := clientset.BatchV1().CronJobs(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	out := make([]resourceSummary, 0, len(list.Items))
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
		out = append(out, resourceSummary{
			Kind:          "cronJob",
			Name:          name,
			Namespace:     nsPtr,
			PrimaryText:   schedule,
			SecondaryText: secondary,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return json.NewEncoder(os.Stdout).Encode(out)
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
