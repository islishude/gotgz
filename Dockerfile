# syntax=docker/dockerfile:1
FROM golang:1.23.5-alpine AS compiler
RUN apk add --no-cache make gcc musl-dev linux-headers git ca-certificates
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build go install ./gotgz

FROM alpine:3.21.2
COPY --from=compiler /go/bin/gotgz /usr/local/bin/
ENTRYPOINT [ "gotgz" ]
