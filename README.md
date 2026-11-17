# 🔨 Pod を倒せ！

Pod をタップして倒すと、Kubernetes がすぐに元に戻す。セルフヒーリングを体で感じるゲームです。
CloudNative Days の WAIWAI広場の [OSS Showcase](https://portal.showcase.cloudnativedays.jp/) 向けに作りました。

- 画面に、的になる Deployment（`pod-whack-target`、6 個の Pod）が六角形で並びます
- タップすると、ゲームのサーバが Kubernetes の API に `DELETE pod` を送ります
- ReplicaSet が新しい Pod を作り、Pending → ContainerCreating → Running と進む様子が、みんなの画面にリアルタイムで出ます
- 倒してから埋め合わせの Pod が Running になるまでの「復活の時間」を測ります

## しくみ

- Go の標準ライブラリだけで書いています。client-go は使わず、REST で Pod の一覧（0.5 秒ごと）と削除をします
- 画面へは Server-Sent Events で配ります
- ゲームのサーバの ServiceAccount は、自分の namespace の Pod に `get`、`list`、`delete` しかできません。消せるのは `TARGET_SELECTOR` に合う Pod だけです
- 連打への備え：1 人あたり 1 秒に 2 回、全体で 1 秒に 4 回まで。Terminating の Pod が `MAX_TERMINATING` 個あるあいだは、次を倒せません（ResourceQuota の Pod 数に収めるため）
- 的の Pod は `registry.k8s.io/pause` で、CPU 5m、メモリ 8Mi しか使いません

## 手元で動かす

クラスタがなくても、偽の API サーバで動かせます。

```bash
FAKE_CLUSTER=1 go run .
```

http://localhost:8080 を開きます。

本物のクラスタで試すときは、`kubectl proxy` を立てて、その先に向けます。

```bash
kubectl apply -f deploy/app.yaml   # HTTPRoute 以外
kubectl proxy &
KUBE_API=http://127.0.0.1:8001 POD_NAMESPACE=default go run .
```

## 設定

| 環境変数 | 既定 | |
|---|---|---|
| `ADDR` | `:8080` | 待ち受けるアドレス |
| `KUBE_API` | `https://kubernetes.default.svc` | API サーバ |
| `POD_NAMESPACE` | ServiceAccount の namespace | |
| `TARGET_SELECTOR` | `app=pod-whack-target` | 倒してよい Pod のラベル |
| `MAX_TERMINATING` | `2` | 同時に Terminating にできる数 |
| `MAX_CLIENTS` | `1000` | 同時に繋げる画面の数 |
| `FAKE_CLUSTER` | | `1` で偽のクラスタを使う |

## イメージ

`v*` のタグを push すると、GitHub Actions が `ghcr.io/tatsukiminami/pod-whack` に amd64 と arm64 のイメージを出します。
