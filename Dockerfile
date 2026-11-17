FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod ./
COPY *.go ./
COPY web ./web
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /pod-whack .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /pod-whack /pod-whack
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/pod-whack"]
