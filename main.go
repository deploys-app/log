package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/acoshift/configfile"
	"github.com/golang-jwt/jwt"
	"github.com/moonrhythm/parapet"
	"github.com/moonrhythm/parapet/pkg/healthz"
	"github.com/moonrhythm/parapet/pkg/logger"

	"github.com/deploys-app/log/k8s"
)

var config = configfile.NewEnvReader()

var k8sClient *k8s.Client

var (
	jwtSecret = config.MustBytes("token")
	location  = config.MustString("location")
	namespace = config.MustString("namespace")
)

func main() {
	var err error
	// k8sClient, err = k8s.NewLocalClient(namespace)
	k8sClient, err = k8s.NewClient(namespace)
	if err != nil {
		slog.Error("cannot create k8s client", "error", err)
		os.Exit(1)
	}

	port := config.StringDefault("PORT", "8080")
	slog.Info("starting server", "port", port)

	go watchPods()
	go watchEvents()

	mux := http.NewServeMux()
	mux.HandleFunc("/", logHandler)
	mux.HandleFunc("/event", eventHandler)
	mux.HandleFunc("/pods", podsHandler)
	mux.HandleFunc("/status", statusHandler)
	// mux.HandleFunc("/proxy", proxyHandler)

	srv := parapet.NewBackend()
	srv.Use(healthz.New())
	srv.Use(logger.Stdout())
	srv.Handler = recoveryMiddleware(mux)
	srv.Addr = ":" + port
	err = srv.ListenAndServe()
	if err != nil {
		slog.Error("starting server failed", "error", err)
		os.Exit(1)
	}
}

func recoveryMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if r := recover(); r != nil {
				if err, ok := r.(error); ok {
					if errors.Is(err, context.Canceled) {
						return
					}
				}

				slog.Error("panic", "error", r)
				debug.PrintStack()
				http.Error(w, "Internal Sever Error", http.StatusInternalServerError)
			}
		}()
		h.ServeHTTP(w, r)
	})
}

func validToken(token string) (subject string, ok bool) {
	token = strings.TrimSpace(token)
	if token == "" {
		return "", false
	}

	tk, err := jwt.ParseWithClaims(token, &jwt.StandardClaims{}, func(token *jwt.Token) (any, error) {
		// TODO: change signing method to ES
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("invalid signing method")
		}

		return jwtSecret, nil
	})
	if err != nil {
		return "", false
	}

	claim := tk.Claims.(*jwt.StandardClaims)
	if claim.Valid() != nil {
		return "", false
	}

	valid := true
	valid = valid && claim.VerifyIssuer("deploys/api", true)
	valid = valid && claim.VerifyAudience("deploys/log:"+location, true)
	valid = valid && claim.Subject != ""
	if !valid {
		return "", false
	}
	return claim.Subject, true
}

func logHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "no-cache")

	podID, ok := validToken(r.FormValue("t"))
	if !ok {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	raw := r.FormValue("raw") == "1"

	responseType := r.FormValue("type")
	if responseType == "" {
		responseType = "sse"
	}

	var send func(l *k8s.LogEntry)
	switch responseType {
	default:
		http.Error(w, "Invalid type parameter", http.StatusBadRequest)
		return
	case "sse":
		send = func(l *k8s.LogEntry) {
			bs, _ := json.Marshal(l)
			fmt.Fprintf(w, "data: %s\n\n", string(bs))
		}
	case "text":
		send = func(l *k8s.LogEntry) {
			fmt.Fprintf(w, "%s %s %s\n",
				l.Timestamp.Format(time.RFC3339),
				l.Pod,
				l.Log,
			)
		}
		if raw {
			send = func(l *k8s.LogEntry) {
				fmt.Fprintf(w, "%s\n",
					l.Log,
				)
			}
		}
	}

	f := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	f.Flush()

	// check pod must > 0
	podExists := false
	podsLocker.RLock()
	for name := range pods {
		if !strings.HasPrefix(name, podID) {
			continue
		}
		podExists = true
		break
	}
	podsLocker.RUnlock()

	if !podExists {
		// back-off
		time.Sleep(2 * time.Second)
		return
	}

	err := k8sClient.Logs(r.Context(), podID, 1000, func(l *k8s.LogEntry) {
		defer func() {
			recover()
		}()

		send(l)
		f.Flush()
	})
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		err = nil
	}
	if err != nil {
		slog.Warn("get logs failed", "pod", podID, "error", err)
	}
}

func podsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "no-cache")

	podID, ok := validToken(r.FormValue("t"))
	if !ok {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	var ps []*k8s.Pod
	podsLocker.RLock()
	for name, p := range pods {
		if !strings.HasPrefix(name, podID) {
			continue
		}
		ps = append(ps, p)
	}
	podsLocker.RUnlock()

	// desc sort
	sort.Slice(ps, func(i, j int) bool {
		return ps[j].Status.StartTime.Before(ps[i].Status.StartTime)
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ps)
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "no-cache")

	podID, ok := validToken(r.FormValue("t"))
	if !ok {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	var status struct {
		Count     int `json:"count"`
		Ready     int `json:"ready"`
		Succeeded int `json:"succeeded"`
		Failed    int `json:"failed"`
	}

	podsLocker.RLock()
	for name, p := range pods {
		if !strings.HasPrefix(name, podID) {
			continue
		}

		// k8s 1.18 scheduler spam with NodeAffinity event
		if p.Status.Phase == "Failed" && p.Status.Reason == "NodeAffinity" {
			continue
		}

		// node preempted
		if p.Status.Phase == "Failed" && p.Status.Reason == "Terminated" && strings.Contains(p.Status.Message, "node shutdown") {
			// message = Pod was terminated in response to imminent node shutdown.
			continue
		}

		// node shutdown
		if p.Status.Phase == "Failed" && p.Status.Reason == "NodeShutdown" && strings.Contains(p.Status.Message, "shutting down") {
			// message = Pod was rejected as the node is shutting down.
			continue
		}

		// skip Evicted pod
		if p.Status.Reason == "Evicted" {
			continue
		}

		status.Count++
		if p.Status.ContainerStatus.Ready {
			status.Ready++
		}
		switch p.Status.Phase {
		case "Succeeded":
			status.Succeeded++
		case "Failed":
			status.Failed++
		}
	}
	podsLocker.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(status)
}

func eventHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "no-cache")

	podID, ok := validToken(r.FormValue("t"))
	if !ok {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	var podEvents []*k8s.Event
	// eventsLocker.RLock()
	// for _, ev := range events {
	// 	if ev.InvolvedObject.Kind != "Pod" {
	// 		continue
	// 	}
	// 	if !strings.HasPrefix(ev.InvolvedObject.Name, podID) {
	// 		continue
	// 	}
	//
	// 	// k8s 1.18 scheduler spam with NodeAffinity event
	// 	if ev.Type == "Warning" && ev.Reason == "NodeAffinity" {
	// 		continue
	// 	}
	//
	// 	podEvents = append(podEvents, ev)
	// }
	// eventsLocker.RUnlock()

	// desc sort
	// sort.Slice(podEvents, func(i, j int) bool {
	// 	return podEvents[j].LastSeen.Before(podEvents[i].LastSeen)
	// })

	events, err := k8sClient.Events(r.Context(), podID)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	if err != nil {
		slog.Error("get events failed", "pod", podID, "error", err)
		http.Error(w, "Error", http.StatusInternalServerError)
		return
	}

	for _, ev := range events {
		// k8s 1.18 scheduler spam with NodeAffinity event
		if ev.Type == "Warning" && ev.Reason == "NodeAffinity" {
			continue
		}

		podEvents = append(podEvents, ev)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(podEvents)
}

var (
	eventsLocker sync.RWMutex
	events       []*k8s.Event
)

func watchEvents() {
	ctx := context.Background()

	storeEventTime := time.Hour

	var cleanup func()
	cleanup = func() {
		defer time.AfterFunc(time.Minute, cleanup)

		now := time.Now()
		cleanBefore := now.Add(-storeEventTime)

		eventsLocker.Lock()
		defer eventsLocker.Unlock()

		var p []*k8s.Event
		for _, ev := range events {
			if ev.LastSeen.Before(cleanBefore) {
				continue
			}
			p = append(p, ev)
		}
		events = p
	}
	time.AfterFunc(time.Minute, cleanup)

	for {
		slog.Info("reset events state")
		eventsLocker.Lock()
		events = nil
		eventsLocker.Unlock()

		err := k8sClient.WatchEvents(ctx, func(ev *k8s.Event) {
			if ev.LastSeen.Before(time.Now().Add(-storeEventTime)) {
				return
			}

			eventsLocker.Lock()
			defer eventsLocker.Unlock()
			events = append(events, ev)
		})
		if err != nil {
			slog.Error("watching events error", "error", err)
		}
		time.Sleep(5 * time.Second)
	}
}

var (
	podsLocker sync.RWMutex
	pods       = map[string]*k8s.Pod{}
)

func watchPods() {
	ctx := context.Background()

	for {
		podsLocker.Lock()
		pods = map[string]*k8s.Pod{}
		podsLocker.Unlock()

		err := k8sClient.WatchPods(ctx, func(et string, p *k8s.Pod) {
			podsLocker.Lock()
			defer podsLocker.Unlock()

			switch et {
			case "ADDED", "MODIFIED":
				pods[p.Name] = p
			case "DELETED":
				delete(pods, p.Name)
			}
		})
		if err != nil {
			slog.Error("watching pods error", "error", err)
		}
		time.Sleep(5 * time.Second)
	}
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	podID, ok := validToken(r.FormValue("t"))
	if !ok {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	port, _ := strconv.Atoi(r.FormValue("port"))
	if port <= 0 {
		http.Error(w, "Invalid Port", http.StatusBadRequest)
		return
	}

	svc := fmt.Sprintf("%s.%s.svc.cluster.local", podID, namespace)
	nc, err := net.Dial("tcp", svc)
	if err != nil {
		http.Error(w, "Can not connect to service", http.StatusBadGateway)
		return
	}
	defer nc.Close()

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Can not switch protocol", http.StatusInternalServerError)
		return
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		http.Error(w, fmt.Sprintf("Hijack error; %v", err), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	ch := make(chan error, 1)
	c := srcDstCopy{conn, nc}
	go c.srcToDst(ch)
	go c.dstToSrc(ch)
	<-ch
}

type srcDstCopy struct {
	src, dst io.ReadWriter
}

func (c srcDstCopy) srcToDst(ch chan<- error) {
	_, err := io.Copy(c.dst, c.src)
	ch <- err
}

func (c srcDstCopy) dstToSrc(ch chan<- error) {
	_, err := io.Copy(c.src, c.dst)
	ch <- err
}
