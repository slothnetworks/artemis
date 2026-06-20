# Multi-stage build: one image, the command selects the role
# (manager | agent | server | client | hashpw).
FROM golang:1.25-alpine AS build
RUN apk add --no-cache git
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/connqc ./cmd/connqc

FROM alpine:3.20
# mtr/traceroute are used by the agent's MTR-on-alert capture (Phase 9); they are
# harmless to include for other roles.
RUN apk add --no-cache mtr traceroute ca-certificates
COPY --from=build /out/connqc /usr/local/bin/connqc
ENTRYPOINT ["connqc"]
