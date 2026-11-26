# infer-router 创空间镜像：两阶段构建，运行层极小
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/infer-router ./cmd/router

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/infer-router /usr/local/bin/infer-router
COPY config.yaml /etc/infer-router/router.yaml
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
EXPOSE 7860
ENTRYPOINT ["/entrypoint.sh"]
