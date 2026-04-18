// rune-k8s-agent is the Rune Kubernetes helper: client-go for operations we prefer not to route through kubectl.
// JSON on stdout matches RuneCore ClusterResourceSummary or DeploymentSummary (see internal/output).
//
// REST paths align with Kubernetes API layout and Rune’s Swift-side KubernetesRESTPath / Swiftkube-style paths.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/rune/rune-k8s-agent/internal/list"
	"github.com/rune/rune-k8s-agent/internal/kube"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
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
		printUsage()
		os.Exit(2)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: rune-k8s-agent list <jobs|cronjobs|daemonsets|statefulsets|deployments|replicasets> --context NAME --namespace NS")
}

func runList(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("list requires a resource (see --help)")
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

	ctx, cancel := context.WithTimeout(context.Background(), kube.DefaultListTimeout)
	defer cancel()

	var rows interface{}
	var err error
	switch resource {
	case "jobs":
		rows, err = list.Jobs(ctx, contextName, namespace)
	case "cronjobs":
		rows, err = list.CronJobs(ctx, contextName, namespace)
	case "daemonsets":
		rows, err = list.DaemonSets(ctx, contextName, namespace)
	case "statefulsets":
		rows, err = list.StatefulSets(ctx, contextName, namespace)
	case "deployments":
		rows, err = list.Deployments(ctx, contextName, namespace)
	case "replicasets":
		rows, err = list.ReplicaSets(ctx, contextName, namespace)
	default:
		return fmt.Errorf("unknown resource %q (use jobs, cronjobs, daemonsets, statefulsets, deployments, replicasets)", resource)
	}
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	return enc.Encode(rows)
}
