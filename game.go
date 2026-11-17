package main

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"
)

// 画面に出す Pod の段階。
const (
	StagePending     = "pending"     // スケジュール待ち
	StageCreating    = "creating"    // コンテナを作っている
	StageRunning     = "running"     // Ready
	StageTerminating = "terminating" // 倒された
	StageError       = "error"       // ImagePullBackOff など
)

type PodView struct {
	Name      string `json:"name"`
	Short     string `json:"short"`
	Stage     string `json:"stage"`
	Reason    string `json:"reason"`
	Node      string `json:"node"`
	CreatedAt int64  `json:"createdAt"`
}

type Event struct {
	At   int64  `json:"at"`
	Kind string `json:"kind"` // kill, born, ready, gone
	Pod  string `json:"pod"`
	Ms   int64  `json:"ms,omitempty"`
}

type State struct {
	Pods       []PodView `json:"pods"`
	Kills      int       `json:"kills"`
	Heals      int       `json:"heals"`
	LastHealMs int64     `json:"lastHealMs"`
	BestHealMs int64     `json:"bestHealMs"`
	AvgHealMs  int64     `json:"avgHealMs"`
	Events     []Event   `json:"events"`
	Error      string    `json:"error,omitempty"`
}

func viewOf(p Pod) PodView {
	v := PodView{
		Name:      p.Metadata.Name,
		Short:     shortName(p.Metadata.Name),
		Node:      p.Spec.NodeName,
		CreatedAt: p.Metadata.CreationTimestamp.UnixMilli(),
	}
	ready, scheduled := false, false
	for _, c := range p.Status.Conditions {
		if c.Type == "Ready" && c.Status == "True" {
			ready = true
		}
		if c.Type == "PodScheduled" && c.Status == "True" {
			scheduled = true
		}
	}
	waiting := ""
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
			waiting = cs.State.Waiting.Reason
		}
	}
	switch {
	case p.Metadata.DeletionTimestamp != nil:
		v.Stage, v.Reason = StageTerminating, "Terminating"
	case p.Status.Phase == "Running" && ready:
		v.Stage, v.Reason = StageRunning, "Running"
	case waiting != "" && waiting != "ContainerCreating" && waiting != "PodInitializing":
		v.Stage, v.Reason = StageError, waiting
	case scheduled || p.Spec.NodeName != "":
		v.Stage, v.Reason = StageCreating, "ContainerCreating"
	default:
		v.Stage, v.Reason = StagePending, "Pending"
	}
	return v
}

// shortName は ReplicaSet が付けた末尾の 5 文字を返す。
func shortName(name string) string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '-' {
			return name[i+1:]
		}
	}
	return name
}

// Game は Pod の様子を見張り、倒した数と立ち直るまでの時間を数える。
type Game struct {
	kube *Kube

	mu         sync.Mutex
	pods       map[string]PodView
	initial    map[string]bool // 起動したときにいた Pod。復活の時間を測らない
	kills      int
	killTimes  []time.Time // まだ埋め合わせの Pod が Ready になっていない kill
	heals      int
	healSum    int64
	lastHealMs int64
	bestHealMs int64
	events     []Event
	errMsg     string
	seeded     bool

	json    []byte
	subs    map[chan []byte]struct{}
	refresh chan struct{}
}

func NewGame(k *Kube) *Game {
	return &Game{
		kube:    k,
		pods:    map[string]PodView{},
		initial: map[string]bool{},
		subs:    map[chan []byte]struct{}{},
		refresh: make(chan struct{}, 1),
		json:    []byte(`{"pods":[],"events":[]}`),
	}
}

const maxEvents = 15

func (g *Game) addEvent(e Event) {
	g.events = append(g.events, e)
	if len(g.events) > maxEvents {
		g.events = g.events[len(g.events)-maxEvents:]
	}
}

// Run は Pod の一覧を繰り返し取り、変わったら購読者に配る。
func (g *Game) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		g.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-g.refresh:
		}
	}
}

func (g *Game) Refresh() {
	select {
	case g.refresh <- struct{}{}:
	default:
	}
}

func (g *Game) poll(ctx context.Context) {
	pods, err := g.kube.ListPods(ctx)
	now := time.Now()

	g.mu.Lock()
	defer g.mu.Unlock()
	if err != nil {
		log.Printf("Pod の一覧を取れない: %v", err)
		g.errMsg = "Kubernetes の API サーバに繋がりません"
		g.publishLocked()
		return
	}
	g.errMsg = ""

	next := make(map[string]PodView, len(pods))
	for _, p := range pods {
		v := viewOf(p)
		next[v.Name] = v
		if !g.seeded {
			g.initial[v.Name] = true
			continue
		}
		old, seen := g.pods[v.Name]
		if !seen {
			g.addEvent(Event{At: now.UnixMilli(), Kind: "born", Pod: v.Short})
		}
		if v.Stage == StageRunning && (!seen || old.Stage != StageRunning) && !g.initial[v.Name] {
			e := Event{At: now.UnixMilli(), Kind: "ready", Pod: v.Short}
			if len(g.killTimes) > 0 {
				ms := now.Sub(g.killTimes[0]).Milliseconds()
				g.killTimes = g.killTimes[1:]
				g.heals++
				g.healSum += ms
				g.lastHealMs = ms
				if g.bestHealMs == 0 || ms < g.bestHealMs {
					g.bestHealMs = ms
				}
				e.Ms = ms
			}
			g.addEvent(e)
		}
	}
	if g.seeded {
		for name, v := range g.pods {
			if _, ok := next[name]; !ok {
				g.addEvent(Event{At: now.UnixMilli(), Kind: "gone", Pod: v.Short})
				delete(g.initial, name)
			}
		}
	}
	// 埋め合わせが来ないまま古くなった kill は、測るのをあきらめる
	for len(g.killTimes) > 0 && now.Sub(g.killTimes[0]) > 2*time.Minute {
		g.killTimes = g.killTimes[1:]
	}
	g.pods = next
	g.seeded = true
	g.publishLocked()
}

func (g *Game) stateLocked() State {
	s := State{
		Kills:      g.kills,
		Heals:      g.heals,
		LastHealMs: g.lastHealMs,
		BestHealMs: g.bestHealMs,
		Events:     append([]Event{}, g.events...),
		Error:      g.errMsg,
	}
	if g.heals > 0 {
		s.AvgHealMs = g.healSum / int64(g.heals)
	}
	for _, v := range g.pods {
		s.Pods = append(s.Pods, v)
	}
	sort.Slice(s.Pods, func(i, j int) bool {
		if s.Pods[i].CreatedAt != s.Pods[j].CreatedAt {
			return s.Pods[i].CreatedAt < s.Pods[j].CreatedAt
		}
		return s.Pods[i].Name < s.Pods[j].Name
	})
	if s.Pods == nil {
		s.Pods = []PodView{}
	}
	return s
}

func (g *Game) publishLocked() {
	b, err := json.Marshal(g.stateLocked())
	if err != nil {
		return
	}
	if string(b) == string(g.json) {
		return
	}
	g.json = b
	for ch := range g.subs {
		select {
		case ch <- b:
		default: // 遅い購読者は飛ばす。次の更新で追いつく
		}
	}
}

func (g *Game) Subscribe() (chan []byte, []byte) {
	ch := make(chan []byte, 4)
	g.mu.Lock()
	g.subs[ch] = struct{}{}
	cur := g.json
	g.mu.Unlock()
	return ch, cur
}

func (g *Game) Unsubscribe(ch chan []byte) {
	g.mu.Lock()
	delete(g.subs, ch)
	g.mu.Unlock()
}

func (g *Game) Subscribers() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.subs)
}

type killResult int

const (
	killOK killResult = iota
	killUnknown
	killAlreadyDown
	killTooMany
)

// CheckKill は、その Pod を今倒してよいかを決める。
// Terminating の Pod も ResourceQuota の Pod 数に数えられるので、同時に倒せる数を絞る。
func (g *Game) CheckKill(name string, maxTerminating int) killResult {
	g.mu.Lock()
	defer g.mu.Unlock()
	v, ok := g.pods[name]
	if !ok {
		return killUnknown
	}
	if v.Stage == StageTerminating {
		return killAlreadyDown
	}
	n := 0
	for _, p := range g.pods {
		if p.Stage == StageTerminating {
			n++
		}
	}
	if n >= maxTerminating {
		return killTooMany
	}
	// 次の一覧を待たずに Terminating にしておく。同じ Pod を二度倒さないため
	v.Stage, v.Reason = StageTerminating, "Terminating"
	g.pods[name] = v
	return killOK
}

func (g *Game) RecordKill(name string, ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !ok {
		return
	}
	g.kills++
	g.killTimes = append(g.killTimes, time.Now())
	g.addEvent(Event{At: time.Now().UnixMilli(), Kind: "kill", Pod: shortName(name)})
	g.publishLocked()
}
