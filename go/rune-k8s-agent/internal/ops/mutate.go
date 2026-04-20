package ops

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/restmapper"

	"github.com/rune/rune-k8s-agent/internal/kube"
)

const deploymentRevisionAnnotation = "deployment.kubernetes.io/revision"

func DeleteResource(ctx context.Context, contextName, namespace, kind, name string) error {
	gvr, namespaced, err := gvrForKubectlKind(kind)
	if err != nil {
		return err
	}
	cfg, err := kube.NewRESTConfig(contextName)
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	if namespaced {
		if namespace == "" {
			return errors.New("namespace is required for namespaced resource deletion")
		}
		return dyn.Resource(gvr).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}
	return dyn.Resource(gvr).Delete(ctx, name, metav1.DeleteOptions{})
}

func ScaleDeployment(ctx context.Context, contextName, namespace, deploymentName string, replicas int32) error {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return err
	}
	deployment, err := clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	deployment.Spec.Replicas = &replicas
	_, err = clientset.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	return err
}

func RolloutRestartDeployment(ctx context.Context, contextName, namespace, deploymentName string, nowRFC3339 string) error {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return err
	}
	deployment, err := clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if deployment.Spec.Template.Annotations == nil {
		deployment.Spec.Template.Annotations = map[string]string{}
	}
	deployment.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = nowRFC3339
	_, err = clientset.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	return err
}

func PatchCronJobSuspend(ctx context.Context, contextName, namespace, name string, suspend bool) error {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return err
	}
	patch := []byte(fmt.Sprintf(`{"spec":{"suspend":%t}}`, suspend))
	_, err = clientset.BatchV1().CronJobs(namespace).Patch(
		ctx,
		name,
		types.MergePatchType,
		patch,
		metav1.PatchOptions{},
	)
	return err
}

func CreateJobFromCronJob(ctx context.Context, contextName, namespace, cronJobName, jobName string) error {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return err
	}
	cron, err := clientset.BatchV1().CronJobs(namespace).Get(ctx, cronJobName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
			Labels:    cloneStringMap(cron.Spec.JobTemplate.Labels),
			Annotations: mergeStringMaps(
				cloneStringMap(cron.Spec.JobTemplate.Annotations),
				map[string]string{"cronjob.kubernetes.io/instantiate": "manual"},
			),
		},
		Spec: *cron.Spec.JobTemplate.Spec.DeepCopy(),
	}
	_, err = clientset.BatchV1().Jobs(namespace).Create(ctx, job, metav1.CreateOptions{})
	return err
}

func ApplyFile(ctx context.Context, contextName, namespace, filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	return ApplyYAML(ctx, contextName, namespace, data)
}

func ApplyYAML(ctx context.Context, contextName, namespace string, yamlBytes []byte) error {
	cfg, err := kube.NewRESTConfig(contextName)
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return err
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))

	decoder := yaml.NewYAMLOrJSONDecoder(strings.NewReader(string(yamlBytes)), 4096)
	for {
		raw := map[string]interface{}{}
		if err := decoder.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if len(raw) == 0 {
			continue
		}
		obj := &unstructured.Unstructured{Object: raw}
		gvk := obj.GroupVersionKind()
		if gvk.Kind == "" || gvk.Version == "" {
			return fmt.Errorf("manifest saknar apiVersion/kind")
		}
		if obj.GetName() == "" {
			return fmt.Errorf("manifest för %s saknar metadata.name", gvk.Kind)
		}
		mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return err
		}
		var res dynamic.ResourceInterface
		if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
			targetNS := obj.GetNamespace()
			if targetNS == "" {
				targetNS = namespace
				obj.SetNamespace(targetNS)
			}
			if targetNS == "" {
				return fmt.Errorf("manifest %s/%s kräver namespace", gvk.Kind, obj.GetName())
			}
			res = dyn.Resource(mapping.Resource).Namespace(targetNS)
		} else {
			res = dyn.Resource(mapping.Resource)
		}

		payload, err := obj.MarshalJSON()
		if err != nil {
			return err
		}
		force := true
		_, err = res.Patch(
			ctx,
			obj.GetName(),
			types.ApplyPatchType,
			payload,
			metav1.PatchOptions{
				FieldManager: "rune-k8s-agent",
				Force:        &force,
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func cloneStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func mergeStringMaps(base map[string]string, extra map[string]string) map[string]string {
	if base == nil && extra == nil {
		return nil
	}
	out := map[string]string{}
	for key, value := range base {
		out[key] = value
	}
	for key, value := range extra {
		out[key] = value
	}
	return out
}

func gvrForKubectlKind(kind string) (schema.GroupVersionResource, bool, error) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "pod", "pods":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}, true, nil
	case "deployment", "deployments":
		return schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}, true, nil
	case "statefulset", "statefulsets":
		return schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}, true, nil
	case "daemonset", "daemonsets":
		return schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"}, true, nil
	case "job", "jobs":
		return schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}, true, nil
	case "cronjob", "cronjobs":
		return schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "cronjobs"}, true, nil
	case "replicaset", "replicasets":
		return schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}, true, nil
	case "service", "services":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}, true, nil
	case "ingress", "ingresses":
		return schema.GroupVersionResource{Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"}, true, nil
	case "configmap", "configmaps":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}, true, nil
	case "secret", "secrets":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}, true, nil
	case "node", "nodes":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}, false, nil
	case "event", "events":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "events"}, true, nil
	case "role", "roles":
		return schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}, true, nil
	case "rolebinding", "rolebindings":
		return schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}, true, nil
	case "clusterrole", "clusterroles":
		return schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}, false, nil
	case "clusterrolebinding", "clusterrolebindings":
		return schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}, false, nil
	case "pvc", "persistentvolumeclaim", "persistentvolumeclaims":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}, true, nil
	case "pv", "persistentvolume", "persistentvolumes":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumes"}, false, nil
	case "storageclass", "storageclasses":
		return schema.GroupVersionResource{Group: "storage.k8s.io", Version: "v1", Resource: "storageclasses"}, false, nil
	case "hpa", "horizontalpodautoscaler", "horizontalpodautoscalers":
		return schema.GroupVersionResource{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"}, true, nil
	case "networkpolicy", "networkpolicies":
		return schema.GroupVersionResource{Group: "networking.k8s.io", Version: "v1", Resource: "networkpolicies"}, true, nil
	default:
		return schema.GroupVersionResource{}, false, fmt.Errorf("unsupported kind for delete: %q", kind)
	}
}

func IsDeployment(appObj metav1.Object, deploymentName string) bool {
	for _, owner := range appObj.GetOwnerReferences() {
		if owner.Kind == "Deployment" && owner.Name == deploymentName {
			return true
		}
	}
	return false
}

func DeploymentRevision(obj metav1.Object) (int, bool) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		return 0, false
	}
	value, ok := annotations[deploymentRevisionAnnotation]
	if !ok {
		return 0, false
	}
	var parsed int
	_, err := fmt.Sscanf(value, "%d", &parsed)
	if err != nil {
		return 0, false
	}
	return parsed, true
}
