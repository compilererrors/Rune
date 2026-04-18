// Package kube holds client-go wiring (kubeconfig + REST). Same auth/exec semantics as kubectl.
package kube

import (
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// DefaultListTimeout matches prior main.go behavior for slow clusters.
const DefaultListTimeout = 120 * time.Second

// NewClientset builds a typed clientset for the named kubeconfig context (KUBECONFIG env, same as kubectl).
func NewClientset(contextName string) (*kubernetes.Clientset, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{CurrentContext: contextName}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)
	restConfig, err := cc.ClientConfig()
	if err != nil {
		return nil, err
	}
	restConfig.Timeout = DefaultListTimeout
	return kubernetes.NewForConfig(restConfig)
}
