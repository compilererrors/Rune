package ops

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/rune/rune-k8s-agent/internal/kube"
	"github.com/rune/rune-k8s-agent/internal/output"
)

var exitCodePattern = regexp.MustCompile(`exit code ([0-9]+)`)

func ExecInPod(
	ctx context.Context,
	contextName, namespace, podName, container string,
	command []string,
) (output.ExecResult, error) {
	if len(command) == 0 {
		return output.ExecResult{}, errors.New("exec requires at least one command argument")
	}
	restConfig, err := kube.NewRESTConfigWithTimeout(contextName, 0)
	if err != nil {
		return output.ExecResult{}, err
	}
	clientset, err := kube.NewClientset(contextName)
	if err != nil {
		return output.ExecResult{}, err
	}

	req := clientset.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec")

	options := &corev1.PodExecOptions{
		Command: command,
		Stdin:   false,
		Stdout:  true,
		Stderr:  true,
		TTY:     false,
	}
	if container != "" {
		options.Container = container
	}
	req.VersionedParams(options, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(restConfig, "POST", req.URL())
	if err != nil {
		return output.ExecResult{}, err
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	streamErr := exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if streamErr == nil {
		return output.ExecResult{
			Stdout:   stdout.String(),
			Stderr:   stderr.String(),
			ExitCode: 0,
		}, nil
	}

	exitCode := int32(1)
	if parsed := parseRemoteExecExitCode(streamErr); parsed != nil {
		exitCode = int32(*parsed)
	}
	return output.ExecResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exitCode,
	}, nil
}

func parseRemoteExecExitCode(err error) *int {
	matches := exitCodePattern.FindStringSubmatch(err.Error())
	if len(matches) != 2 {
		return nil
	}
	value, parseErr := strconv.Atoi(matches[1])
	if parseErr != nil {
		return nil
	}
	return &value
}

func ExecErrorFromResult(result output.ExecResult) error {
	if result.ExitCode == 0 {
		return nil
	}
	msg := result.Stderr
	if msg == "" {
		msg = result.Stdout
	}
	if msg == "" {
		msg = fmt.Sprintf("command exited with code %d", result.ExitCode)
	}
	return fmt.Errorf(msg)
}
