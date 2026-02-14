# syntax=docker/dockerfile:1
FROM golang:1.26.0-alpine AS compiler
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build go install ./gotgz

FROM alpine:latest
COPY --from=compiler /go/bin/gotgz /usr/local/bin/
ENTRYPOINT [ "gotgz" ]
