package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

//go:embed web
var webFS embed.FS

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func main() {
	addr := env("ADDR", ":8080")
	api := env("KUBE_API", "https://kubernetes.default.svc")
	selector := env("TARGET_SELECTOR", "app=pod-whack-target")
	maxTerminating := envInt("MAX_TERMINATING", 2)
	maxSubs := envInt("MAX_CLIENTS", 1000)

	// FAKE_CLUSTER=1 のときは、クラスタの代わりに手元で動く偽の API サーバを使う
	if os.Getenv("FAKE_CLUSTER") == "1" {
		fake := NewFakeCluster(6, "pod-whack-target")
		srv := httptest.NewServer(fake)
		defer srv.Close()
		api = srv.URL
		os.Setenv("POD_NAMESPACE", "fake")
		log.Printf("偽のクラスタで動きます: %s", api)
	}

	kube, err := NewKube(api, os.Getenv("POD_NAMESPACE"), selector)
	if err != nil {
		log.Fatal(err)
	}
	game := NewGame(kube)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go game.Run(ctx, 500*time.Millisecond)

	global := newBucket(4, 4)           // クラスタ全体で 1 秒に 4 回まで
	perClient := newClientLimiter(2, 3) // 1 人あたり 1 秒に 2 回まで（3 連打までは通す）

	mux := http.NewServeMux()
	static, _ := fs.Sub(webFS, "web")
	mux.Handle("GET /", http.FileServerFS(static))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })

	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		fl, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		if game.Subscribers() >= maxSubs {
			http.Error(w, "混み合っています", http.StatusServiceUnavailable)
			return
		}
		ch, cur := game.Subscribe()
		defer game.Unsubscribe(ch)
		h := w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("X-Accel-Buffering", "no")
		send := func(b []byte) bool {
			if _, err := w.Write([]byte("data: ")); err != nil {
				return false
			}
			w.Write(b)
			w.Write([]byte("\n\n"))
			fl.Flush()
			return true
		}
		if !send(cur) {
			return
		}
		ping := time.NewTicker(20 * time.Second)
		defer ping.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case b := <-ch:
				if !send(b) {
					return
				}
			case <-ping.C:
				if _, err := w.Write([]byte(": ping\n\n")); err != nil {
					return
				}
				fl.Flush()
			}
		}
	})

	mux.HandleFunc("POST /api/kill", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Pod string `json:"pod"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&req); err != nil || req.Pod == "" {
			reply(w, http.StatusBadRequest, "どの Pod か分かりません")
			return
		}
		if !perClient.allow(clientIP(r)) {
			reply(w, http.StatusTooManyRequests, "ちょっと待って！連打しすぎです")
			return
		}
		switch game.CheckKill(req.Pod, maxTerminating) {
		case killUnknown:
			reply(w, http.StatusNotFound, "その Pod はもういません")
			return
		case killAlreadyDown:
			reply(w, http.StatusConflict, "もう倒れています")
			return
		case killTooMany:
			reply(w, http.StatusTooManyRequests, "Kubernetes が立て直し中です。少し待って！")
			return
		}
		if !global.allow() {
			game.Refresh()
			reply(w, http.StatusTooManyRequests, "みんなが叩きすぎて順番待ちです")
			return
		}
		dctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		err := kube.DeletePod(dctx, req.Pod)
		var ae *apiError
		if errors.As(err, &ae) && ae.Code == http.StatusNotFound {
			err = nil // ちょうど消えたところ。倒したことにする
		}
		game.RecordKill(req.Pod, err == nil)
		game.Refresh()
		if err != nil {
			log.Printf("Pod %s を消せない: %v", req.Pod, err)
			reply(w, http.StatusBadGateway, "倒せませんでした")
			return
		}
		reply(w, http.StatusOK, "倒した！")
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Printf("listening on %s (selector %q)", addr, selector)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func reply(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{"ok": code == http.StatusOK, "message": msg})
}

// clientIP は Gateway が付けた X-Forwarded-For の先頭を使う。
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// bucket はトークンバケット。
type bucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newBucket(rate, burst float64) *bucket {
	return &bucket{rate: rate, burst: burst, tokens: burst, last: time.Now()}
}

func (b *bucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens = min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

type clientLimiter struct {
	mu      sync.Mutex
	rate    float64
	burst   float64
	clients map[string]*bucket
	swept   time.Time
}

func newClientLimiter(rate, burst float64) *clientLimiter {
	return &clientLimiter{rate: rate, burst: burst, clients: map[string]*bucket{}, swept: time.Now()}
}

func (c *clientLimiter) allow(key string) bool {
	c.mu.Lock()
	if time.Since(c.swept) > time.Minute {
		for k, b := range c.clients {
			if time.Since(b.last) > time.Minute {
				delete(c.clients, k)
			}
		}
		c.swept = time.Now()
	}
	b, ok := c.clients[key]
	if !ok {
		b = newBucket(c.rate, c.burst)
		c.clients[key] = b
	}
	c.mu.Unlock()
	return b.allow()
}
