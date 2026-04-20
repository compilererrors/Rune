// rune-k8s-agent is the Rune Kubernetes helper: client-go for operations we prefer not to route through kubectl.
// JSON on stdout matches RuneCore models used on the Swift side.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rune/rune-k8s-agent/internal/kube"
	"github.com/rune/rune-k8s-agent/internal/list"
	"github.com/rune/rune-k8s-agent/internal/ops"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "contexts":
		if err := runContexts(); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "namespaces":
		if err := runNamespaces(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "context-namespace":
		if err := runContextNamespace(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "list":
		if err := runList(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "top":
		if err := runTop(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "logs":
		if err := runLogs(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "selector":
		if err := runSelector(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "exec":
		if err := runExec(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "delete":
		if err := runDelete(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "scale":
		if err := runScale(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "rollout":
		if err := runRollout(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "apply":
		if err := runApply(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "patch":
		if err := runPatch(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "create":
		if err := runCreate(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "port-forward":
		if err := runPortForward(os.Args[2:]); err != nil {
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
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent contexts")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent namespaces --context NAME")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent context-namespace --context NAME")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent list <pods|services|ingresses|configmaps|events|secrets|roles|rolebindings|networkpolicies|horizontalpodautoscalers|persistentvolumeclaims|nodes|persistentvolumes|storageclasses|clusterroles|clusterrolebindings|jobs|cronjobs|daemonsets|statefulsets|deployments|replicasets> --context NAME [--namespace NS]")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent top <nodes|pods> --context NAME [--namespace NS|--all-namespaces]")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent logs --context NAME --namespace NS --pod POD [--container C] [--tail N] [--since 5m] [--since-time RFC3339] [--previous]")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent selector <service|deployment> --context NAME --namespace NS --name NAME")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent selector pods --context NAME --namespace NS --label-selector key=value,...")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent exec --context NAME --namespace NS --pod POD [--container C] -- <command...>")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent delete --context NAME --namespace NS --kind KIND --name NAME")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent scale deployment --context NAME --namespace NS --name NAME --replicas N")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent rollout <restart|history|undo> deployment --context NAME --namespace NS --name NAME [--to-revision N]")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent apply --context NAME --namespace NS --file PATH")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent patch cronjob-suspend --context NAME --namespace NS --name NAME --suspend <true|false>")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent create job-from-cronjob --context NAME --namespace NS --cronjob NAME --job NAME")
	fmt.Fprintln(os.Stderr, "  rune-k8s-agent port-forward --context NAME --namespace NS --target-kind <pod|service> --target-name NAME --local-port N --remote-port N [--address 127.0.0.1]")
}

func writeJSON(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

func runContexts() error {
	rows, err := list.Contexts()
	if err != nil {
		return err
	}
	return writeJSON(rows)
}

func runNamespaces(args []string) error {
	fs := flag.NewFlagSet("namespaces", flag.ContinueOnError)
	var contextName string
	fs.StringVar(&contextName, "context", "", "kubeconfig context name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if contextName == "" {
		return fmt.Errorf("--context is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), kube.DefaultListTimeout)
	defer cancel()
	rows, err := list.Namespaces(ctx, contextName)
	if err != nil {
		return err
	}
	return writeJSON(rows)
}

func runContextNamespace(args []string) error {
	fs := flag.NewFlagSet("context-namespace", flag.ContinueOnError)
	var contextName string
	fs.StringVar(&contextName, "context", "", "kubeconfig context name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if contextName == "" {
		return fmt.Errorf("--context is required")
	}
	ns, err := list.ContextNamespace(contextName)
	if err != nil {
		return err
	}
	return writeJSON(map[string]string{"namespace": ns})
}

func runList(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("list requires a resource (see --help)")
	}
	resource := args[0]
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	var contextName, namespace string
	var allNamespaces bool
	fs.StringVar(&contextName, "context", "", "kubeconfig context name")
	fs.StringVar(&namespace, "namespace", "", "namespace")
	fs.BoolVar(&allNamespaces, "all-namespaces", false, "list across all namespaces")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if contextName == "" {
		return fmt.Errorf("--context is required")
	}
	if namespace == "" && !allNamespaces && resourceNeedsNamespace(resource) {
		return fmt.Errorf("--namespace is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), kube.DefaultListTimeout)
	defer cancel()

	var rows interface{}
	var err error
	switch resource {
	case "pods":
		if allNamespaces {
			rows, err = list.Pods(ctx, contextName, metav1.NamespaceAll)
		} else {
			rows, err = list.Pods(ctx, contextName, namespace)
		}
	case "services":
		if allNamespaces {
			rows, err = list.Services(ctx, contextName, metav1.NamespaceAll)
		} else {
			rows, err = list.Services(ctx, contextName, namespace)
		}
	case "ingresses":
		if allNamespaces {
			rows, err = list.Ingresses(ctx, contextName, metav1.NamespaceAll)
		} else {
			rows, err = list.Ingresses(ctx, contextName, namespace)
		}
	case "configmaps":
		if allNamespaces {
			rows, err = list.ConfigMaps(ctx, contextName, metav1.NamespaceAll)
		} else {
			rows, err = list.ConfigMaps(ctx, contextName, namespace)
		}
	case "secrets":
		if allNamespaces {
			rows, err = list.Secrets(ctx, contextName, metav1.NamespaceAll)
		} else {
			rows, err = list.Secrets(ctx, contextName, namespace)
		}
	case "roles":
		rows, err = list.Roles(ctx, contextName, namespace)
	case "rolebindings":
		rows, err = list.RoleBindings(ctx, contextName, namespace)
	case "networkpolicies":
		rows, err = list.NetworkPolicies(ctx, contextName, namespace)
	case "horizontalpodautoscalers":
		rows, err = list.HorizontalPodAutoscalers(ctx, contextName, namespace)
	case "persistentvolumeclaims":
		rows, err = list.PersistentVolumeClaims(ctx, contextName, namespace)
	case "events":
		if allNamespaces {
			rows, err = list.Events(ctx, contextName, metav1.NamespaceAll)
		} else {
			rows, err = list.Events(ctx, contextName, namespace)
		}
	case "nodes":
		rows, err = list.Nodes(ctx, contextName)
	case "persistentvolumes":
		rows, err = list.PersistentVolumes(ctx, contextName)
	case "storageclasses":
		rows, err = list.StorageClasses(ctx, contextName)
	case "clusterroles":
		rows, err = list.ClusterRoles(ctx, contextName)
	case "clusterrolebindings":
		rows, err = list.ClusterRoleBindings(ctx, contextName)
	case "jobs":
		rows, err = list.Jobs(ctx, contextName, namespace)
	case "cronjobs":
		rows, err = list.CronJobs(ctx, contextName, namespace)
	case "daemonsets":
		rows, err = list.DaemonSets(ctx, contextName, namespace)
	case "statefulsets":
		rows, err = list.StatefulSets(ctx, contextName, namespace)
	case "deployments":
		if allNamespaces {
			rows, err = list.Deployments(ctx, contextName, metav1.NamespaceAll)
		} else {
			rows, err = list.Deployments(ctx, contextName, namespace)
		}
	case "replicasets":
		rows, err = list.ReplicaSets(ctx, contextName, namespace)
	default:
		return fmt.Errorf("unknown resource %q", resource)
	}
	if err != nil {
		return err
	}
	return writeJSON(rows)
}

func runTop(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("top requires a resource (nodes|pods)")
	}
	resource := args[0]
	fs := flag.NewFlagSet("top", flag.ContinueOnError)
	var contextName, namespace string
	var allNamespaces bool
	fs.StringVar(&contextName, "context", "", "kubeconfig context name")
	fs.StringVar(&namespace, "namespace", "", "namespace")
	fs.BoolVar(&allNamespaces, "all-namespaces", false, "top across all namespaces")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if contextName == "" {
		return fmt.Errorf("--context is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	switch resource {
	case "nodes":
		row, err := ops.TopNodesPercent(ctx, contextName)
		if err != nil {
			return err
		}
		return writeJSON(row)
	case "pods":
		if allNamespaces {
			rows, err := ops.TopPodsAllNamespaces(ctx, contextName)
			if err != nil {
				return err
			}
			return writeJSON(rows)
		}
		if namespace == "" {
			return fmt.Errorf("--namespace is required unless --all-namespaces is set")
		}
		rows, err := ops.TopPods(ctx, contextName, namespace)
		if err != nil {
			return err
		}
		return writeJSON(rows)
	default:
		return fmt.Errorf("unknown top resource %q", resource)
	}
}

func runLogs(args []string) error {
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	var contextName, namespace, podName, container, since, sinceTime string
	var tail int
	var previous, follow bool
	fs.StringVar(&contextName, "context", "", "kubeconfig context name")
	fs.StringVar(&namespace, "namespace", "", "namespace")
	fs.StringVar(&podName, "pod", "", "pod name")
	fs.StringVar(&container, "container", "", "container name")
	fs.StringVar(&since, "since", "", "duration, e.g. 5m")
	fs.StringVar(&sinceTime, "since-time", "", "RFC3339 timestamp")
	fs.IntVar(&tail, "tail", 0, "tail lines")
	fs.BoolVar(&previous, "previous", false, "previous container")
	fs.BoolVar(&follow, "follow", false, "follow logs")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if contextName == "" || namespace == "" || podName == "" {
		return fmt.Errorf("--context, --namespace and --pod are required")
	}
	ctx := context.Background()
	opts := ops.PodLogsOptions{
		Container:  container,
		Previous:   previous,
		Follow:     follow,
		Timestamps: true,
	}
	if tail > 0 {
		v := int64(tail)
		opts.TailLines = &v
	}
	if since != "" {
		d, err := time.ParseDuration(since)
		if err != nil {
			return fmt.Errorf("invalid --since value: %w", err)
		}
		opts.Since = &d
	}
	if sinceTime != "" {
		parsed, err := time.Parse(time.RFC3339, sinceTime)
		if err != nil {
			return fmt.Errorf("invalid --since-time value: %w", err)
		}
		t := metav1.NewTime(parsed)
		opts.SinceTime = &t
	}
	return ops.PodLogs(ctx, contextName, namespace, podName, opts, os.Stdout)
}

func runSelector(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("selector requires a target (service|deployment|pods)")
	}
	target := args[0]
	fs := flag.NewFlagSet("selector", flag.ContinueOnError)
	var contextName, namespace, name, labelSelector string
	fs.StringVar(&contextName, "context", "", "kubeconfig context name")
	fs.StringVar(&namespace, "namespace", "", "namespace")
	fs.StringVar(&name, "name", "", "resource name")
	fs.StringVar(&labelSelector, "label-selector", "", "label selector")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if contextName == "" || namespace == "" {
		return fmt.Errorf("--context and --namespace are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), kube.DefaultListTimeout)
	defer cancel()

	switch target {
	case "service":
		if name == "" {
			return fmt.Errorf("--name is required for selector service")
		}
		selector, err := ops.ServiceSelector(ctx, contextName, namespace, name)
		if err != nil {
			return err
		}
		return writeJSON(selector)
	case "deployment":
		if name == "" {
			return fmt.Errorf("--name is required for selector deployment")
		}
		selector, err := ops.DeploymentSelector(ctx, contextName, namespace, name)
		if err != nil {
			return err
		}
		return writeJSON(selector)
	case "pods":
		if strings.TrimSpace(labelSelector) == "" {
			return fmt.Errorf("--label-selector is required for selector pods")
		}
		pods, err := ops.PodsBySelector(ctx, contextName, namespace, labelSelector)
		if err != nil {
			return err
		}
		return writeJSON(pods)
	default:
		return fmt.Errorf("unknown selector target %q", target)
	}
}

func runExec(args []string) error {
	fs := flag.NewFlagSet("exec", flag.ContinueOnError)
	var contextName, namespace, podName, container string
	fs.StringVar(&contextName, "context", "", "kubeconfig context name")
	fs.StringVar(&namespace, "namespace", "", "namespace")
	fs.StringVar(&podName, "pod", "", "pod name")
	fs.StringVar(&container, "container", "", "container")

	separator := -1
	for index, arg := range args {
		if arg == "--" {
			separator = index
			break
		}
	}
	if separator == -1 {
		return fmt.Errorf("exec requires command after --")
	}
	if err := fs.Parse(args[:separator]); err != nil {
		return err
	}
	command := args[separator+1:]
	if contextName == "" || namespace == "" || podName == "" {
		return fmt.Errorf("--context, --namespace and --pod are required")
	}
	ctx := context.Background()
	result, err := ops.ExecInPod(ctx, contextName, namespace, podName, container, command)
	if err != nil {
		return err
	}
	return writeJSON(result)
}

func runDelete(args []string) error {
	fs := flag.NewFlagSet("delete", flag.ContinueOnError)
	var contextName, namespace, kind, name string
	fs.StringVar(&contextName, "context", "", "kubeconfig context")
	fs.StringVar(&namespace, "namespace", "", "namespace")
	fs.StringVar(&kind, "kind", "", "resource kind")
	fs.StringVar(&name, "name", "", "resource name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if contextName == "" || kind == "" || name == "" {
		return fmt.Errorf("--context, --kind and --name are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), kube.DefaultListTimeout)
	defer cancel()
	return ops.DeleteResource(ctx, contextName, namespace, kind, name)
}

func runScale(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("scale requires a target (deployment)")
	}
	if args[0] != "deployment" {
		return fmt.Errorf("scale currently supports deployment only")
	}
	fs := flag.NewFlagSet("scale", flag.ContinueOnError)
	var contextName, namespace, name string
	var replicas int
	fs.StringVar(&contextName, "context", "", "kubeconfig context")
	fs.StringVar(&namespace, "namespace", "", "namespace")
	fs.StringVar(&name, "name", "", "deployment name")
	fs.IntVar(&replicas, "replicas", -1, "replica count")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if contextName == "" || namespace == "" || name == "" || replicas < 0 {
		return fmt.Errorf("--context, --namespace, --name and non-negative --replicas are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), kube.DefaultListTimeout)
	defer cancel()
	return ops.ScaleDeployment(ctx, contextName, namespace, name, int32(replicas))
}

func runRollout(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("rollout requires action and target (restart|history|undo deployment)")
	}
	action := args[0]
	target := args[1]
	if target != "deployment" {
		return fmt.Errorf("rollout supports deployment only")
	}
	fs := flag.NewFlagSet("rollout", flag.ContinueOnError)
	var contextName, namespace, name string
	var toRevision int
	var hasToRevision bool
	fs.StringVar(&contextName, "context", "", "kubeconfig context")
	fs.StringVar(&namespace, "namespace", "", "namespace")
	fs.StringVar(&name, "name", "", "deployment name")
	fs.Func("to-revision", "target revision", func(value string) error {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		toRevision = parsed
		hasToRevision = true
		return nil
	})
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	if contextName == "" || namespace == "" || name == "" {
		return fmt.Errorf("--context, --namespace and --name are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), kube.DefaultListTimeout)
	defer cancel()

	switch action {
	case "restart":
		return ops.RolloutRestartDeployment(ctx, contextName, namespace, name, time.Now().UTC().Format(time.RFC3339))
	case "history":
		text, err := ops.DeploymentRolloutHistoryText(ctx, contextName, namespace, name)
		if err != nil {
			return err
		}
		_, err = io.WriteString(os.Stdout, text)
		return err
	case "undo":
		var revisionPtr *int
		if hasToRevision {
			revisionPtr = &toRevision
		}
		return ops.RolloutUndoDeployment(ctx, contextName, namespace, name, revisionPtr)
	default:
		return fmt.Errorf("unsupported rollout action %q", action)
	}
}

func runApply(args []string) error {
	fs := flag.NewFlagSet("apply", flag.ContinueOnError)
	var contextName, namespace, filePath string
	fs.StringVar(&contextName, "context", "", "kubeconfig context")
	fs.StringVar(&namespace, "namespace", "", "namespace fallback")
	fs.StringVar(&filePath, "file", "", "manifest file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if contextName == "" || filePath == "" {
		return fmt.Errorf("--context and --file are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), kube.DefaultListTimeout)
	defer cancel()
	return ops.ApplyFile(ctx, contextName, namespace, filePath)
}

func runPatch(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("patch requires a target (cronjob-suspend)")
	}
	if args[0] != "cronjob-suspend" {
		return fmt.Errorf("patch supports cronjob-suspend only")
	}
	fs := flag.NewFlagSet("patch", flag.ContinueOnError)
	var contextName, namespace, name string
	var suspend bool
	fs.StringVar(&contextName, "context", "", "kubeconfig context")
	fs.StringVar(&namespace, "namespace", "", "namespace")
	fs.StringVar(&name, "name", "", "cronjob name")
	fs.BoolVar(&suspend, "suspend", false, "suspend value")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if contextName == "" || namespace == "" || name == "" {
		return fmt.Errorf("--context, --namespace and --name are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), kube.DefaultListTimeout)
	defer cancel()
	return ops.PatchCronJobSuspend(ctx, contextName, namespace, name, suspend)
}

func runCreate(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("create requires a target (job-from-cronjob)")
	}
	if args[0] != "job-from-cronjob" {
		return fmt.Errorf("create supports job-from-cronjob only")
	}
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	var contextName, namespace, cronJobName, jobName string
	fs.StringVar(&contextName, "context", "", "kubeconfig context")
	fs.StringVar(&namespace, "namespace", "", "namespace")
	fs.StringVar(&cronJobName, "cronjob", "", "cronjob name")
	fs.StringVar(&jobName, "job", "", "job name")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if contextName == "" || namespace == "" || cronJobName == "" || jobName == "" {
		return fmt.Errorf("--context, --namespace, --cronjob and --job are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), kube.DefaultListTimeout)
	defer cancel()
	return ops.CreateJobFromCronJob(ctx, contextName, namespace, cronJobName, jobName)
}

func runPortForward(args []string) error {
	fs := flag.NewFlagSet("port-forward", flag.ContinueOnError)
	var contextName, namespace, targetKind, targetName, address string
	var localPort, remotePort int
	fs.StringVar(&contextName, "context", "", "kubeconfig context")
	fs.StringVar(&namespace, "namespace", "", "namespace")
	fs.StringVar(&targetKind, "target-kind", "", "pod or service")
	fs.StringVar(&targetName, "target-name", "", "resource name")
	fs.IntVar(&localPort, "local-port", 0, "local port")
	fs.IntVar(&remotePort, "remote-port", 0, "remote port")
	fs.StringVar(&address, "address", "127.0.0.1", "bind address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if contextName == "" || namespace == "" || targetKind == "" || targetName == "" || localPort <= 0 || remotePort <= 0 {
		return fmt.Errorf("--context, --namespace, --target-kind, --target-name, --local-port and --remote-port are required")
	}
	if targetKind != "pod" && targetKind != "service" {
		return fmt.Errorf("--target-kind must be pod or service")
	}
	ctx := context.Background()
	return ops.PortForward(ctx, contextName, namespace, targetKind, targetName, localPort, remotePort, address)
}

func resourceNeedsNamespace(resource string) bool {
	switch resource {
	case "nodes", "persistentvolumes", "storageclasses", "clusterroles", "clusterrolebindings":
		return false
	default:
		return true
	}
}
