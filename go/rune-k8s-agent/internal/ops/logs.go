package ops

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/rune/rune-k8s-agent/internal/kube"
	"github.com/rune/rune-k8s-agent/internal/output"
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
	_, err := kube.NewRESTConfigWithTimeout(contextName, 0)
	if err != nil {
		return err
	}
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return err
	}
	return streamPodLogs(ctx, clientset, namespace, podName, opts, out)
}

type UnifiedLogsOptions struct {
	LabelSelector string
	MaxPods       int
	Concurrency   int
	PodLogs       PodLogsOptions
}

type taggedLogLine struct {
	PodName   string
	Text      string
	Timestamp *time.Time
}

func UnifiedLogsBySelector(ctx context.Context, contextName, namespace string, opts UnifiedLogsOptions) (output.UnifiedLogs, error) {
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return output.UnifiedLogs{}, err
	}
	list, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: opts.LabelSelector,
	})
	if err != nil {
		return output.UnifiedLogs{}, err
	}

	pods := make([]output.PodSummary, 0, len(list.Items))
	for _, pod := range list.Items {
		if pod.Name == "" {
			continue
		}
		status := string(pod.Status.Phase)
		if status == "" {
			status = "Unknown"
		}
		pods = append(pods, output.PodSummary{
			Name:      pod.Name,
			Namespace: namespace,
			Status:    status,
		})
	}

	selected := selectPodsForUnifiedLogs(pods, opts.MaxPods)
	if len(selected) == 0 {
		return output.UnifiedLogs{PodNames: []string{}, MergedText: ""}, nil
	}

	lines, err := collectUnifiedPodLogLines(ctx, clientset, namespace, selected, opts)
	if err != nil {
		return output.UnifiedLogs{}, err
	}
	sort.Slice(lines, func(i, j int) bool {
		return taggedLineLess(lines[i], lines[j])
	})

	mergedLines := make([]string, 0, len(lines))
	for _, line := range lines {
		mergedLines = append(mergedLines, "["+line.PodName+"] "+line.Text)
	}

	podNames := make([]string, 0, len(selected))
	for _, pod := range selected {
		podNames = append(podNames, pod.Name)
	}
	sort.Strings(podNames)

	return output.UnifiedLogs{
		PodNames:   podNames,
		MergedText: strings.Join(mergedLines, "\n"),
	}, nil
}

func streamPodLogs(ctx context.Context, clientset kubernetes.Interface, namespace, podName string, opts PodLogsOptions, out io.Writer) error {
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
	return nil
}

func selectPodsForUnifiedLogs(pods []output.PodSummary, maxPods int) []output.PodSummary {
	if len(pods) == 0 {
		return nil
	}
	active := make([]output.PodSummary, 0, len(pods))
	for _, pod := range pods {
		status := strings.ToLower(strings.TrimSpace(pod.Status))
		if status == "running" || status == "pending" || status == "unknown" {
			active = append(active, pod)
		}
	}
	source := active
	if len(source) == 0 {
		source = pods
	}
	sort.Slice(source, func(i, j int) bool { return source[i].Name < source[j].Name })
	limit := maxPods
	if limit <= 0 {
		limit = len(source)
	}
	if limit > len(source) {
		limit = len(source)
	}
	return append([]output.PodSummary(nil), source[:limit]...)
}

func collectUnifiedPodLogLines(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace string,
	pods []output.PodSummary,
	opts UnifiedLogsOptions,
) ([]taggedLogLine, error) {
	if len(pods) == 0 {
		return nil, nil
	}

	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}

	sem := make(chan struct{}, concurrency)
	results := make(chan []taggedLogLine, len(pods))
	errs := make(chan error, len(pods))
	var wg sync.WaitGroup

	for _, pod := range pods {
		pod := pod
		wg.Add(1)
		go func() {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			}
			defer func() { <-sem }()

			var buf bytes.Buffer
			if err := streamPodLogs(ctx, clientset, namespace, pod.Name, opts.PodLogs, &buf); err != nil {
				if ctx.Err() != nil {
					errs <- ctx.Err()
				}
				return
			}
			results <- taggedLinesFromLogs(buf.String(), pod.Name)
		}()
	}

	go func() {
		wg.Wait()
		close(results)
		close(errs)
	}()

	lines := make([]taggedLogLine, 0)
	for result := range results {
		lines = append(lines, result...)
	}

	for err := range errs {
		if err != nil {
			return nil, err
		}
	}

	return lines, nil
}

func taggedLinesFromLogs(logs, podName string) []taggedLogLine {
	rawLines := strings.FieldsFunc(logs, func(r rune) bool {
		return r == '\n' || r == '\r'
	})
	lines := make([]taggedLogLine, 0, len(rawLines))
	for _, line := range rawLines {
		if line == "" {
			continue
		}
		lines = append(lines, taggedLogLine{
			PodName:   podName,
			Text:      line,
			Timestamp: parseTimestamp(line),
		})
	}
	return lines
}

func parseTimestamp(line string) *time.Time {
	token := strings.Fields(line)
	if len(token) == 0 {
		return nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, token[0]); err == nil {
		return &parsed
	}
	if parsed, err := time.Parse(time.RFC3339, token[0]); err == nil {
		return &parsed
	}
	return nil
}

func taggedLineLess(lhs, rhs taggedLogLine) bool {
	switch {
	case lhs.Timestamp != nil && rhs.Timestamp != nil:
		if !lhs.Timestamp.Equal(*rhs.Timestamp) {
			return lhs.Timestamp.Before(*rhs.Timestamp)
		}
	case lhs.Timestamp == nil && rhs.Timestamp != nil:
		return false
	case lhs.Timestamp != nil && rhs.Timestamp == nil:
		return true
	}

	if lhs.PodName != rhs.PodName {
		return lhs.PodName < rhs.PodName
	}
	return lhs.Text < rhs.Text
}
