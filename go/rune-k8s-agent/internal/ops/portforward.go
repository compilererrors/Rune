package ops

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"syscall"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"

	"github.com/rune/rune-k8s-agent/internal/kube"
)

func PortForward(
	ctx context.Context,
	contextName, namespace, targetKind, targetName string,
	localPort, remotePort int,
	address string,
) error {
	restConfig, err := kube.NewRESTConfigWithTimeout(contextName, 0)
	if err != nil {
		return err
	}
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return err
	}

	podName := targetName
	if targetKind == "service" {
		resolved, err := resolveServicePod(ctx, clientset, namespace, targetName)
		if err != nil {
			return err
		}
		podName = resolved
	}
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward", namespace, podName)

	serverURL, err := url.Parse(restConfig.Host)
	if err != nil {
		return err
	}
	serverURL.Path = path

	transport, upgrader, err := spdy.RoundTripperFor(restConfig)
	if err != nil {
		return err
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", serverURL)

	stopChan := make(chan struct{})
	readyChan := make(chan struct{})
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	go func() {
		select {
		case <-signals:
			close(stopChan)
		case <-ctx.Done():
			close(stopChan)
		}
	}()

	ports := []string{fmt.Sprintf("%d:%d", localPort, remotePort)}
	forwarder, err := portforward.NewOnAddresses(
		dialer,
		[]string{address},
		ports,
		stopChan,
		readyChan,
		os.Stdout,
		os.Stderr,
	)
	if err != nil {
		return err
	}

	return forwarder.ForwardPorts()
}

func resolveServicePod(ctx context.Context, clientset *kubernetes.Clientset, namespace, serviceName string) (string, error) {
	service, err := clientset.CoreV1().Services(namespace).Get(ctx, serviceName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if len(service.Spec.Selector) == 0 {
		return "", fmt.Errorf("service %s saknar selector", serviceName)
	}
	selectorParts := make([]string, 0, len(service.Spec.Selector))
	for key, value := range service.Spec.Selector {
		selectorParts = append(selectorParts, fmt.Sprintf("%s=%s", key, value))
	}
	sort.Strings(selectorParts)
	selector := ""
	for i, part := range selectorParts {
		if i > 0 {
			selector += ","
		}
		selector += part
	}

	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return "", err
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("inga pods hittades för service %s", serviceName)
	}
	// Prefer Running pod, otherwise first available.
	for _, pod := range pods.Items {
		if pod.Status.Phase == "Running" && pod.Name != "" {
			return pod.Name, nil
		}
	}
	return pods.Items[0].Name, nil
}
