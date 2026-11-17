package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const saDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// Pod は API サーバが返す Pod のうち、ゲームで使うところだけを持つ。
type Pod struct {
	Metadata struct {
		Name              string     `json:"name"`
		CreationTimestamp time.Time  `json:"creationTimestamp"`
		DeletionTimestamp *time.Time `json:"deletionTimestamp,omitempty"`
	} `json:"metadata"`
	Spec struct {
		NodeName string `json:"nodeName"`
	} `json:"spec"`
	Status struct {
		Phase      string `json:"phase"`
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
		ContainerStatuses []struct {
			State struct {
				Waiting *struct {
					Reason string `json:"reason"`
				} `json:"waiting,omitempty"`
			} `json:"state"`
		} `json:"containerStatuses"`
	} `json:"status"`
}

type podList struct {
	Items []Pod `json:"items"`
}

// Kube は client-go を使わず、標準ライブラリだけで Pod の一覧と削除をする。
type Kube struct {
	base      string
	namespace string
	selector  string
	token     string
	http      *http.Client
}

func NewKube(base, namespace, selector string) (*Kube, error) {
	k := &Kube{base: strings.TrimRight(base, "/"), namespace: namespace, selector: selector}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if b, err := os.ReadFile(saDir + "/token"); err == nil {
		k.token = strings.TrimSpace(string(b))
	}
	if ca, err := os.ReadFile(saDir + "/ca.crt"); err == nil {
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca)
		tr.TLSClientConfig = &tls.Config{RootCAs: pool}
	}
	if k.namespace == "" {
		b, err := os.ReadFile(saDir + "/namespace")
		if err != nil {
			return nil, fmt.Errorf("namespace が分からない（POD_NAMESPACE を設定してください）: %w", err)
		}
		k.namespace = strings.TrimSpace(string(b))
	}
	k.http = &http.Client{Transport: tr, Timeout: 5 * time.Second}
	return k, nil
}

func (k *Kube) do(ctx context.Context, method, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, k.base+path, nil)
	if err != nil {
		return nil, err
	}
	if k.token != "" {
		req.Header.Set("Authorization", "Bearer "+k.token)
	}
	req.Header.Set("Accept", "application/json")
	res, err := k.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 300 {
		return body, &apiError{Code: res.StatusCode, Body: string(body)}
	}
	return body, nil
}

type apiError struct {
	Code int
	Body string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("API サーバが %d を返した: %.200s", e.Code, e.Body)
}

func (k *Kube) ListPods(ctx context.Context) ([]Pod, error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods?labelSelector=%s", url.PathEscape(k.namespace), url.QueryEscape(k.selector))
	body, err := k.do(ctx, http.MethodGet, path)
	if err != nil {
		return nil, err
	}
	var l podList
	if err := json.Unmarshal(body, &l); err != nil {
		return nil, err
	}
	return l.Items, nil
}

func (k *Kube) DeletePod(ctx context.Context, name string) error {
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", url.PathEscape(k.namespace), url.PathEscape(name))
	_, err := k.do(ctx, http.MethodDelete, path)
	return err
}
