package ops

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/rune/rune-k8s-agent/internal/kube"
)

func ResourceDescribe(ctx context.Context, contextName, namespace, kind, name string) (string, error) {
	obj, err := getResourceObject(ctx, contextName, namespace, kind, name)
	if err != nil {
		return "", err
	}

	var out strings.Builder
	fmt.Fprintf(&out, "Name:\t%s\n", obj.GetName())
	if obj.GetNamespace() != "" {
		fmt.Fprintf(&out, "Namespace:\t%s\n", obj.GetNamespace())
	}
	fmt.Fprintf(&out, "Kind:\t%s\n", obj.GetKind())
	fmt.Fprintf(&out, "API Version:\t%s\n", obj.GetAPIVersion())
	created := obj.GetCreationTimestamp()
	if !created.Time.IsZero() {
		fmt.Fprintf(&out, "Created:\t%s\n", created.Format("2006-01-02T15:04:05Z07:00"))
	}

	writeStringMapSection(&out, "Labels", obj.GetLabels())
	writeStringMapSection(&out, "Annotations", obj.GetAnnotations())

	if status, ok, _ := unstructured.NestedMap(obj.Object, "status"); ok && len(status) > 0 {
		out.WriteString("\nStatus:\n")
		writeYAMLBlock(&out, status)
	}

	if owners := obj.GetOwnerReferences(); len(owners) > 0 {
		out.WriteString("\nControlled By:\n")
		for _, owner := range owners {
			fmt.Fprintf(&out, "  %s/%s\n", owner.Kind, owner.Name)
		}
	}

	return out.String(), nil
}

func ResourceYAML(ctx context.Context, contextName, namespace, kind, name string) (string, error) {
	obj, err := getResourceObject(ctx, contextName, namespace, kind, name)
	if err != nil {
		return "", err
	}
	data, err := sigsyaml.Marshal(obj.Object)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func getResourceObject(ctx context.Context, contextName, namespace, kind, name string) (*unstructured.Unstructured, error) {
	gvr, namespaced, err := gvrForKubectlKind(kind)
	if err != nil {
		return nil, err
	}
	cfg, err := kube.NewRESTConfig(contextName)
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	if namespaced {
		if namespace == "" {
			return nil, errors.New("namespace is required for namespaced resource")
		}
		return dyn.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	}
	return dyn.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
}

func writeStringMapSection(out *strings.Builder, title string, values map[string]string) {
	if len(values) == 0 {
		return
	}
	out.WriteString("\n" + title + ":\n")
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(out, "  %s: %s\n", key, values[key])
	}
}

func writeYAMLBlock(out *strings.Builder, value interface{}) {
	data, err := sigsyaml.Marshal(value)
	if err != nil {
		fmt.Fprintf(out, "  %v\n", value)
		return
	}
	for _, line := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		fmt.Fprintf(out, "  %s\n", line)
	}
}
