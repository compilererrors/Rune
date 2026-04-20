package ops

import (
	"context"
	"fmt"
	"io"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rune/rune-k8s-agent/internal/kube"
)

type PodLogsOptions struct {
	Container  string
	Previous   bool
	Follow     bool
	TailLines  *int64
	Since      *time.Duration
	SinceTime  *metav1.Time
	Timestamps bool
}

func PodLogs(ctx context.Context, contextName, namespace, podName string, opts PodLogsOptions, out io.Writer) error {
	restConfig, err := kube.NewRESTConfigWithTimeout(contextName, 0)
	if err != nil {
		return err
	}
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return err
	}
	logsOpts := &corev1.PodLogOptions{
		Container:  opts.Container,
		Previous:   opts.Previous,
		Follow:     opts.Follow,
		Timestamps: opts.Timestamps,
	}
	if opts.TailLines != nil {
		logsOpts.TailLines = opts.TailLines
	}
	if opts.Since != nil {
		secs := int64(opts.Since.Seconds())
		logsOpts.SinceSeconds = &secs
	}
	if opts.SinceTime != nil {
		logsOpts.SinceTime = opts.SinceTime
	}

	// Use streaming for both follow and one-shot reads to avoid loading large payloads in memory.
	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, logsOpts)
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := req.Stream(streamCtx)
	if err != nil {
		return err
	}
	defer stream.Close()

	_, err = io.Copy(out, stream)
	if err != nil {
		return fmt.Errorf("copy pod logs: %w", err)
	}
	_ = restConfig // keep explicit config build parity with kubectl semantics.
	return nil
}
