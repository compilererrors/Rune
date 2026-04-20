package ops

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rune/rune-k8s-agent/internal/kube"
)

type DeploymentRolloutRevision struct {
	Revision    int
	ChangeCause string
}

func DeploymentRolloutHistory(ctx context.Context, contextName, namespace, deploymentName string) ([]DeploymentRolloutRevision, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return nil, err
	}
	rsList, err := clientset.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	revisions := make([]DeploymentRolloutRevision, 0, len(rsList.Items))
	seen := map[int]bool{}
	for _, rs := range rsList.Items {
		if !isOwnedByDeployment(rs, deploymentName) {
			continue
		}
		revision, ok := parseRevision(rs.Annotations[deploymentRevisionAnnotation])
		if !ok || seen[revision] {
			continue
		}
		seen[revision] = true
		changeCause := rs.Annotations["kubernetes.io/change-cause"]
		if strings.TrimSpace(changeCause) == "" {
			changeCause = "<none>"
		}
		revisions = append(revisions, DeploymentRolloutRevision{
			Revision:    revision,
			ChangeCause: changeCause,
		})
	}
	sort.Slice(revisions, func(i, j int) bool { return revisions[i].Revision < revisions[j].Revision })
	return revisions, nil
}

func DeploymentRolloutHistoryText(ctx context.Context, contextName, namespace, deploymentName string) (string, error) {
	revisions, err := DeploymentRolloutHistory(ctx, contextName, namespace, deploymentName)
	if err != nil {
		return "", err
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("deployment.apps/%s\n", deploymentName))
	builder.WriteString("REVISION  CHANGE-CAUSE\n")
	for _, row := range revisions {
		builder.WriteString(fmt.Sprintf("%-8d  %s\n", row.Revision, row.ChangeCause))
	}
	return strings.TrimRight(builder.String(), "\n"), nil
}

func RolloutUndoDeployment(ctx context.Context, contextName, namespace, deploymentName string, toRevision *int) error {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return err
	}
	deployments := clientset.AppsV1().Deployments(namespace)
	deployment, err := deployments.Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	rsList, err := clientset.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	revisionToTemplate := map[int]appsv1.ReplicaSet{}
	for _, rs := range rsList.Items {
		if !isOwnedByDeployment(rs, deploymentName) {
			continue
		}
		revision, ok := parseRevision(rs.Annotations[deploymentRevisionAnnotation])
		if !ok {
			continue
		}
		revisionToTemplate[revision] = rs
	}
	if len(revisionToTemplate) == 0 {
		return fmt.Errorf("ingen rollout-historik hittades för deployment %s", deploymentName)
	}

	var targetRevision int
	if toRevision != nil {
		targetRevision = *toRevision
	} else {
		current := 0
		if parsed, ok := parseRevision(deployment.Annotations[deploymentRevisionAnnotation]); ok {
			current = parsed
		}
		previous := 0
		for revision := range revisionToTemplate {
			if revision < current && revision > previous {
				previous = revision
			}
		}
		if previous == 0 {
			// Fallback: choose latest available revision.
			for revision := range revisionToTemplate {
				if revision > previous {
					previous = revision
				}
			}
		}
		targetRevision = previous
	}

	targetRS, ok := revisionToTemplate[targetRevision]
	if !ok {
		return fmt.Errorf("revision %d hittades inte för deployment %s", targetRevision, deploymentName)
	}

	deployment.Spec.Template = *targetRS.Spec.Template.DeepCopy()
	_, err = deployments.Update(ctx, deployment, metav1.UpdateOptions{})
	return err
}

func isOwnedByDeployment(rs appsv1.ReplicaSet, deploymentName string) bool {
	for _, owner := range rs.OwnerReferences {
		if owner.Kind == "Deployment" && owner.Name == deploymentName {
			return true
		}
	}
	return false
}

func parseRevision(raw string) (int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return v, true
}
