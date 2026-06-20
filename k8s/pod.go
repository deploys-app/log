package k8s

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/utils/pointer"
)

// primaryContainerName is the canonical primary container the deployer creates
// for every workload; the projection and log reads pin it by name so a sidecar
// can't masquerade as the app's health or output.
const primaryContainerName = "app"

func (c *Client) GetPods(ctx context.Context, id string) ([]v1.Pod, error) {
	resp, err := c.client.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("id=%s", id),
	})
	if err != nil {
		return nil, err
	}
	return resp.Items, nil
}

func (c *Client) WatchPods(ctx context.Context, f func(eventType string, p *Pod)) error {
	w, err := c.client.CoreV1().Pods(c.namespace).Watch(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	defer w.Stop()

	for p := range w.ResultChan() {
		obj, _ := p.Object.(*v1.Pod)
		if obj == nil {
			continue
		}

		res := Pod{
			Name: obj.Name,
			Status: PodStatus{
				Phase:      string(obj.Status.Phase),
				Conditions: nil,
				Message:    obj.Status.Message,
				Reason:     obj.Status.Reason,
				NodeName:   obj.Status.NominatedNodeName,
				PodIP:      obj.Status.PodIP,
				StartTime:  optV1TimeToTime(obj.Status.StartTime),
			},
		}
		for _, x := range obj.Status.Conditions {
			res.Status.Conditions = append(res.Status.Conditions, PodCondition{
				Type:               string(x.Type),
				Status:             string(x.Status),
				LastProbeTime:      x.LastProbeTime.Time,
				LastTransitionTime: x.LastTransitionTime.Time,
				Reason:             x.Reason,
				Message:            x.Message,
			})
		}
		if x := pickAppContainerStatus(obj.Status.ContainerStatuses); x != nil {
			res.Status.ContainerStatus = projectContainerStatus(*x)
		}
		f(string(p.Type), &res)
	}
	return nil
}

type Pod struct {
	Name   string    `json:"name"`
	Status PodStatus `json:"status"`
}

type PodStatus struct {
	Phase           string          `json:"phase"`
	Conditions      []PodCondition  `json:"conditions"`
	Message         string          `json:"message"`
	Reason          string          `json:"reason"`
	NodeName        string          `json:"nodeName"`
	PodIP           string          `json:"podIp"`
	StartTime       time.Time       `json:"startTime"`
	ContainerStatus ContainerStatus `json:"containerStatus"`
}

type PodCondition struct {
	Type               string    `json:"type"`
	Status             string    `json:"status"`
	LastProbeTime      time.Time `json:"lastProbeTime"`
	LastTransitionTime time.Time `json:"lastTransitionTime"`
	Reason             string    `json:"reason"`
	Message            string    `json:"message"`
}

type ContainerStatus struct {
	Ready        bool   `json:"ready"`
	RestartCount int    `json:"restartCount"`
	Image        string `json:"image"`
	ImageID      string `json:"imageId"`
	Started      bool   `json:"started"`

	// Failure detail for the 'app' container. State.Waiting carries the
	// current reason (CrashLoopBackOff, ImagePullBackOff, ...); Terminated
	// carries the exit of the running instance; LastTerminated preserves the
	// exit code / OOM signal of the previous instance, which is the only
	// place the cause survives once a crash-looping pod is back in Waiting.
	WaitingReason          string `json:"waitingReason,omitempty"`
	WaitingMessage         string `json:"waitingMessage,omitempty"`
	TerminatedReason       string `json:"terminatedReason,omitempty"`
	TerminatedExitCode     int32  `json:"terminatedExitCode,omitempty"`
	LastTerminatedReason   string `json:"lastTerminatedReason,omitempty"`
	LastTerminatedExitCode int32  `json:"lastTerminatedExitCode,omitempty"`
}

// pickAppContainerStatus returns the status of the primary container (named
// "app" by the deployer). Pods may carry sidecars whose index is not
// guaranteed, so positional [0] is wrong; fall back to [0] only when no "app"
// container exists, preserving prior behaviour for single-container pods.
func pickAppContainerStatus(statuses []v1.ContainerStatus) *v1.ContainerStatus {
	for i := range statuses {
		if statuses[i].Name == "app" {
			return &statuses[i]
		}
	}
	if len(statuses) > 0 {
		return &statuses[0]
	}
	return nil
}

func projectContainerStatus(x v1.ContainerStatus) ContainerStatus {
	cs := ContainerStatus{
		Ready:        x.Ready,
		RestartCount: int(x.RestartCount),
		Image:        x.Image,
		ImageID:      x.ImageID,
		Started:      pointer.BoolDeref(x.Started, false),
	}
	if w := x.State.Waiting; w != nil {
		cs.WaitingReason = w.Reason
		cs.WaitingMessage = w.Message
	}
	if t := x.State.Terminated; t != nil {
		cs.TerminatedReason = t.Reason
		cs.TerminatedExitCode = t.ExitCode
	}
	if lt := x.LastTerminationState.Terminated; lt != nil {
		cs.LastTerminatedReason = lt.Reason
		cs.LastTerminatedExitCode = lt.ExitCode
	}
	return cs
}

// LogsOptions controls a Logs read.
//
// Follow selects the streaming path (the console SSE), which tails the live
// container indefinitely. !Follow selects a bounded snapshot that reads each
// pod's tail to completion and returns — the shape an agent/CLI consumes as a
// single result. Previous reads the last-terminated ("previous") container
// instead of the running one — the crash post-mortem. Pod, when set, restricts
// the read to a single pod of the deployment.
type LogsOptions struct {
	Follow    bool
	TailLines int64
	Previous  bool
	Pod       string
}

func (c *Client) Logs(ctx context.Context, id string, opts LogsOptions, each func(l *LogEntry)) error {
	slog.InfoContext(ctx, "k8s/logs: getting logs", "id", id, "follow", opts.Follow, "previous", opts.Previous)

	pods, err := c.GetPods(ctx, id)
	if err != nil {
		return err
	}

	slog.InfoContext(ctx, "k8s/logs: found pods", "id", id, "count", len(pods))
	if len(pods) == 0 {
		return nil
	}

	var selected []v1.Pod
	for _, pod := range pods {
		if opts.Pod != "" && pod.Name != opts.Pod {
			continue
		}

		// k8s 1.18 scheduler spam with NodeAffinity event
		if pod.Status.Phase == "Failed" && pod.Status.Reason == "NodeAffinity" {
			continue
		}

		// skip Evicted pod (no container logs to read)
		if pod.Status.Reason == "Evicted" {
			continue
		}

		// The follow path streams a live container, so a Terminated pod (whose
		// stream returns immediately) was historically skipped. The snapshot
		// path WANTS terminated pods — that's exactly where a crashed
		// container's logs live — so it does not skip them.
		if opts.Follow && pod.Status.Reason == "Terminated" {
			continue
		}

		selected = append(selected, pod)
	}
	if len(selected) == 0 {
		return nil
	}

	if !opts.Follow {
		return c.logsSnapshot(ctx, selected, opts, each)
	}
	return c.logsFollow(ctx, selected, opts, each)
}

// logsFollow streams the live "app" container of each pod until the context is
// cancelled (the console SSE path). It keeps the original first-EOF-cancels
// semantics, which are correct for follow reads where EOF effectively never
// happens.
func (c *Client) logsFollow(ctx context.Context, pods []v1.Pod, opts LogsOptions, each func(l *LogEntry)) error {
	s := c.client.CoreV1().Pods(c.namespace)

	chEach := make(chan *LogEntry)
	defer close(chEach)
	go func() {
		for e := range chEach {
			each(e)
		}
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	eg, ctx := errgroup.WithContext(ctx)
	for _, pod := range pods {
		eg.Go(func() error {
			stream, err := s.GetLogs(pod.Name, &v1.PodLogOptions{
				Container:  primaryContainerName,
				Timestamps: true,
				Follow:     true,
				TailLines:  pointer.Int64(opts.TailLines),
			}).Stream(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "k8s/logs: can not stream GetLogs", "pod", pod.Name, "err", err)
				return err
			}
			defer stream.Close()

			r := bufio.NewReader(stream)

			chLine := make(chan string)
			go func() {
				defer cancel()

				for {
					line, _, err := r.ReadLine()
					if err != nil {
						return
					}
					chLine <- string(line)
				}
			}()
			for {
				var line string
				select {
				case line = <-chLine:
				case <-ctx.Done():
					return ctx.Err()
				}

				if l := parseLogEntry(pod.Name, line); l != nil {
					chEach <- l
				}
			}
		})
	}
	return eg.Wait()
}

// logsSnapshot reads each pod's tail to completion concurrently and emits the
// collected lines pod-by-pod. Unlike the follow path it must NOT cancel
// siblings on the first pod's EOF: with Follow:false every reader EOFs almost
// immediately, and a first-EOF cancel would truncate the others mid-drain. It
// is best-effort — a per-pod read error (e.g. no previous container) is logged
// and skipped, not propagated.
func (c *Client) logsSnapshot(ctx context.Context, pods []v1.Pod, opts LogsOptions, each func(l *LogEntry)) error {
	results := make([][]*LogEntry, len(pods))

	var wg sync.WaitGroup
	for i := range pods {
		wg.Add(1)
		// i is passed explicitly so each goroutine owns its result slot — correct
		// regardless of loop-variable scoping assumptions.
		go func(i int) {
			defer wg.Done()
			entries, err := c.readPodLogTail(ctx, pods[i].Name, opts)
			if err != nil {
				slog.WarnContext(ctx, "k8s/logs: read tail failed", "pod", pods[i].Name, "err", err)
				return
			}
			results[i] = entries
		}(i)
	}
	wg.Wait()

	// emit sequentially so the caller's callback need not be goroutine-safe.
	for _, entries := range results {
		for _, e := range entries {
			each(e)
		}
	}
	return nil
}

func (c *Client) readPodLogTail(ctx context.Context, podName string, opts LogsOptions) ([]*LogEntry, error) {
	s := c.client.CoreV1().Pods(c.namespace)
	stream, err := s.GetLogs(podName, &v1.PodLogOptions{
		Container:  primaryContainerName,
		Timestamps: true,
		Follow:     false,
		Previous:   opts.Previous,
		TailLines:  pointer.Int64(opts.TailLines),
	}).Stream(ctx)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	var entries []*LogEntry
	r := bufio.NewReader(stream)
	for {
		line, err := r.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		if line != "" {
			if l := parseLogEntry(podName, line); l != nil {
				entries = append(entries, l)
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return entries, err
		}
	}
	return entries, nil
}

// parseLogEntry splits a "<rfc3339nano-ts> <log...>" line (k8s Timestamps:true)
// into a LogEntry. Returns nil for an empty line.
func parseLogEntry(podName, line string) *LogEntry {
	p := strings.SplitN(line, " ", 2)
	if len(p) < 1 || p[0] == "" {
		return nil
	}
	l := &LogEntry{Pod: podName}
	l.Timestamp, _ = time.Parse(time.RFC3339Nano, p[0])
	if len(p) == 2 {
		l.Log = p[1]
	}
	return l
}

type LogEntry struct {
	Pod       string    `json:"pod"`
	Timestamp time.Time `json:"timestamp"`
	Log       string    `json:"log"`
}

func (c *Client) Events(ctx context.Context, id string) ([]*Event, error) {
	slog.InfoContext(ctx, "k8s/events: getting event", "id", id)

	pods, err := c.GetPods(ctx, id)
	if err != nil {
		return nil, err
	}

	slog.InfoContext(ctx, "k8s/events: found pods", "id", id, "count", len(pods))
	if len(pods) == 0 {
		return nil, nil
	}

	var events []*Event
	s := c.client.CoreV1().Events(c.namespace)
	for _, pod := range pods {
		resp, err := s.List(ctx, metav1.ListOptions{
			FieldSelector: fmt.Sprintf("involvedObject.name=%s", pod.Name),
		})
		if err != nil {
			return nil, err
		}

		for _, ev := range resp.Items {
			events = append(events, &Event{
				LastSeen: ev.LastTimestamp.Time,
				Type:     ev.Type,
				Reason:   ev.Reason,
				Message:  ev.Message,
			})
		}
	}

	return events, nil
}

func (c *Client) WatchEvents(ctx context.Context, f func(ev *Event)) error {
	slog.InfoContext(ctx, "k8s/events: watching event")

	w, err := c.client.CoreV1().Events(c.namespace).Watch(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	defer w.Stop()

	for r := range w.ResultChan() {
		switch r.Type {
		case watch.Added, watch.Modified:
		default:
			continue
		}

		ev := r.Object.(*v1.Event)
		f(&Event{
			LastSeen: ev.LastTimestamp.Time,
			Type:     ev.Type,
			Reason:   ev.Reason,
			Message:  ev.Message,
			InvolvedObject: EventObjectReference{
				Kind: ev.InvolvedObject.Kind,
				Name: ev.InvolvedObject.Name,
			},
		})
	}
	return nil
}

type Event struct {
	LastSeen time.Time `json:"lastSeen"`
	Type     string    `json:"type"`
	Reason   string    `json:"reason"`
	Message  string    `json:"message"`

	InvolvedObject EventObjectReference `json:"-"`
}

type EventObjectReference struct {
	Kind string
	Name string
}

func optV1TimeToTime(t *metav1.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.Time
}
