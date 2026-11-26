FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
RUN go build -o /out/infer-router ./cmd/router

FROM alpine:3.20
COPY --from=build /out/infer-router /usr/local/bin/infer-router
COPY config/router.yaml /etc/infer-router/router.yaml
EXPOSE 8080
ENTRYPOINT ["infer-router", "-config", "/etc/infer-router/router.yaml"]
