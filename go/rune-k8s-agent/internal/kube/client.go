// Package kube holds client-go wiring (kubeconfig + REST). Same auth/exec semantics as kubectl.
package kube

import (
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// DefaultListTimeout matches prior main.go behavior for slow clusters.
const DefaultListTimeout = 120 * time.Second

// NewRESTConfig builds a REST config for the named kubeconfig context.
func NewRESTConfig(contextName string) (*rest.Config, error) {
	return NewRESTConfigWithTimeout(contextName, DefaultListTimeout)
}

// NewRESTConfigWithTimeout builds a REST config for the named kubeconfig context with explicit timeout.
// Use timeout 0 for streaming operations (logs -f, exec, port-forward).
func NewRESTConfigWithTimeout(contextName string, timeout time.Duration) (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{CurrentContext: contextName}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)
	restConfig, err := cc.ClientConfig()
	if err != nil {
		return nil, err
	}
	restConfig.Timeout = timeout
	return restConfig, nil
}

// NewClientset builds a typed clientset for the named kubeconfig context (KUBECONFIG env, same as kubectl).
func NewClientset(contextName string) (*kubernetes.Clientset, error) {
	restConfig, err := NewRESTConfig(contextName)
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(restConfig)
}
