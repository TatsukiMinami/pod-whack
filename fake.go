package main

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"
)

// FakeCluster は、ReplicaSet と kubelet のふるまいを真似た小さな偽の API サーバ。
// 手元で画面を作るときと、テストで使う。
type FakeCluster struct {
	mu       sync.Mutex
	replicas int
	prefix   string
	pods     map[string]*fakePod
	// 段階ごとにかかる時間
	Schedule, Create, Terminate time.Duration
}

type fakePod struct {
	name      string
	created   time.Time
	deletedAt *time.Time
}

func NewFakeCluster(replicas int, prefix string) *FakeCluster {
	f := &FakeCluster{
		replicas: replicas, prefix: prefix, pods: map[string]*fakePod{},
		Schedule: 300 * time.Millisecond, Create: 1200 * time.Millisecond, Terminate: 1000 * time.Millisecond,
	}
	past := time.Now().Add(-time.Hour)
	for range replicas {
		f.addLocked(past)
	}
	return f
}

const fakeChars = "bcdfghjklmnpqrstvwxz2456789"

func (f *FakeCluster) addLocked(at time.Time) {
	b := make([]byte, 5)
	for i := range b {
		b[i] = fakeChars[rand.IntN(len(fakeChars))]
	}
	name := fmt.Sprintf("%s-7d9f8c6b5-%s", f.prefix, b)
	f.pods[name] = &fakePod{name: name, created: at}
}

// reconcileLocked は、消し終えた Pod を除き、足りない分を作る。
func (f *FakeCluster) reconcileLocked(now time.Time) {
	alive := 0
	for n, p := range f.pods {
		if p.deletedAt != nil {
			if now.Sub(*p.deletedAt) > f.Terminate {
				delete(f.pods, n)
			}
			continue
		}
		alive++
	}
	for ; alive < f.replicas; alive++ {
		f.addLocked(now)
	}
}

func (f *FakeCluster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	f.reconcileLocked(now)
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// api/v1/namespaces/{ns}/pods[/{name}]
	if len(parts) < 5 || parts[4] != "pods" {
		http.NotFound(w, r)
		return
	}
	switch {
	case r.Method == http.MethodGet && len(parts) == 5:
		items := []map[string]any{}
		for _, p := range f.pods {
			items = append(items, f.render(p, now))
		}
		json.NewEncoder(w).Encode(map[string]any{"items": items})
	case r.Method == http.MethodDelete && len(parts) == 6:
		p, ok := f.pods[parts[5]]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"reason":"NotFound"}`))
			return
		}
		if p.deletedAt == nil {
			p.deletedAt = &now
		}
		f.reconcileLocked(now)
		w.Write([]byte(`{}`))
	default:
		http.Error(w, "unsupported", http.StatusMethodNotAllowed)
	}
}

func (f *FakeCluster) render(p *fakePod, now time.Time) map[string]any {
	age := now.Sub(p.created)
	meta := map[string]any{"name": p.name, "creationTimestamp": p.created.UTC().Format(time.RFC3339)}
	if p.deletedAt != nil {
		meta["deletionTimestamp"] = p.deletedAt.UTC().Format(time.RFC3339)
	}
	spec := map[string]any{}
	status := map[string]any{"phase": "Pending"}
	switch {
	case age < f.Schedule:
		status["conditions"] = []map[string]string{{"type": "PodScheduled", "status": "False"}}
	case age < f.Schedule+f.Create:
		spec["nodeName"] = "fake-node"
		status["conditions"] = []map[string]string{{"type": "PodScheduled", "status": "True"}, {"type": "Ready", "status": "False"}}
		status["containerStatuses"] = []map[string]any{{"state": map[string]any{"waiting": map[string]string{"reason": "ContainerCreating"}}}}
	default:
		spec["nodeName"] = "fake-node"
		status["phase"] = "Running"
		status["conditions"] = []map[string]string{{"type": "PodScheduled", "status": "True"}, {"type": "Ready", "status": "True"}}
		status["containerStatuses"] = []map[string]any{{"state": map[string]any{"running": map[string]any{}}}}
	}
	return map[string]any{"metadata": meta, "spec": spec, "status": status}
}
