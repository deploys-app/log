package k8s

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/utils/pointer"
)

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

func (c *Client) Logs(ctx context.Context, id string, tailLines int64, each func(l *LogEntry)) error {
	slog.InfoContext(ctx, "k8s/logs: getting logs", "id", id)

	s := c.client.CoreV1().Pods(c.namespace)

	pods, err := c.GetPods(ctx, id)
	if err != nil {
		return err
	}

	slog.InfoContext(ctx, "k8s/logs: found pods", "id", id, "count", len(pods))
	if len(pods) == 0 {
		return nil
	}

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
		// k8s 1.18 scheduler spam with NodeAffinity event
		if pod.Status.Phase == "Failed" && pod.Status.Reason == "NodeAffinity" {
			continue
		}

		// skip Evicted pod
		if pod.Status.Reason == "Evicted" {
			continue
		}

		// skip Terminated pod
		if pod.Status.Reason == "Terminated" {
			continue
		}

		eg.Go(func() error {
			stream, err := s.GetLogs(pod.Name, &v1.PodLogOptions{
				Container:  "app",
				Timestamps: true,
				Follow:     true,
				TailLines:  pointer.Int64(tailLines),
			}).Stream(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "k8s/logs: can not stream GetLogs", "id", id, "err", err)
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
					slog.InfoContext(ctx, "k8s/logs: context done", "id", id, "err", err)
					return ctx.Err()
				}

				p := strings.SplitN(line, " ", 2)
				if lp := len(p); lp >= 1 {
					var l LogEntry
					l.Pod = pod.Name
					l.Timestamp, _ = time.Parse(time.RFC3339Nano, p[0])
					if lp == 2 {
						l.Log = p[1]
					}
					chEach <- &l
				}
			}
		})
	}
	return eg.Wait()
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
