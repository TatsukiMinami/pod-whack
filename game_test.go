package main

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestGame(t *testing.T) (*Game, *Kube, *FakeCluster) {
	t.Helper()
	fake := NewFakeCluster(6, "pod-whack-target")
	fake.Schedule, fake.Create, fake.Terminate = 20*time.Millisecond, 50*time.Millisecond, 40*time.Millisecond
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	k, err := NewKube(srv.URL, "test", "app=pod-whack-target")
	if err != nil {
		t.Fatal(err)
	}
	return NewGame(k), k, fake
}

func TestKillAndHeal(t *testing.T) {
	g, k, _ := newTestGame(t)
	ctx := context.Background()
	g.poll(ctx)
	s := g.stateLocked()
	if len(s.Pods) != 6 {
		t.Fatalf("Pod が 6 個のはずが %d 個", len(s.Pods))
	}
	victim := s.Pods[0].Name
	if r := g.CheckKill(victim, 2); r != killOK {
		t.Fatalf("倒せるはず: %v", r)
	}
	if r := g.CheckKill(victim, 2); r != killAlreadyDown {
		t.Fatalf("二度は倒せないはず: %v", r)
	}
	if err := k.DeletePod(ctx, victim); err != nil {
		t.Fatal(err)
	}
	g.RecordKill(victim, true)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		g.poll(ctx)
		if g.heals == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	s = g.stateLocked()
	if s.Kills != 1 || s.Heals != 1 || s.LastHealMs <= 0 {
		t.Fatalf("倒した数と復活が数えられていない: %+v", s)
	}
	running := 0
	for _, p := range s.Pods {
		if p.Name == victim {
			t.Fatalf("倒した Pod %s が残っている", victim)
		}
		if p.Stage == StageRunning {
			running++
		}
	}
	if running != 6 {
		t.Fatalf("6 個とも Running に戻るはずが %d 個", running)
	}
	kinds := map[string]bool{}
	for _, e := range s.Events {
		kinds[e.Kind] = true
	}
	for _, want := range []string{"kill", "born", "ready", "gone"} {
		if !kinds[want] {
			t.Errorf("イベント %q が出ていない: %+v", want, s.Events)
		}
	}
}

func TestMaxTerminating(t *testing.T) {
	g, _, _ := newTestGame(t)
	g.poll(context.Background())
	s := g.stateLocked()
	if g.CheckKill(s.Pods[0].Name, 2) != killOK || g.CheckKill(s.Pods[1].Name, 2) != killOK {
		t.Fatal("2 個までは倒せるはず")
	}
	if r := g.CheckKill(s.Pods[2].Name, 2); r != killTooMany {
		t.Fatalf("3 個目は止めるはず: %v", r)
	}
	if r := g.CheckKill("nope", 2); r != killUnknown {
		t.Fatalf("知らない Pod は断るはず: %v", r)
	}
}

func TestBucket(t *testing.T) {
	b := newBucket(1, 2)
	if !b.allow() || !b.allow() || b.allow() {
		t.Fatal("burst 2 のあとは止めるはず")
	}
}
