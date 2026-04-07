FROM --platform=$BUILDPLATFORM docker.io/golang:1.26.1-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/

FROM builder AS build-primary
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /primary ./cmd/primary

FROM builder AS build-replicator
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /replicator ./cmd/replicator

FROM builder AS build-replica
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /replica ./cmd/replica

FROM builder AS build-api
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /api-server ./cmd/api

FROM docker.io/alpine:3.20 AS runtime

FROM runtime AS primary
COPY --from=build-primary /primary /usr/local/bin/primary
CMD ["primary"]

FROM runtime AS replicator
COPY --from=build-replicator /replicator /usr/local/bin/replicator
EXPOSE 8081
CMD ["replicator"]

FROM runtime AS replica
COPY --from=build-replica /replica /usr/local/bin/replica
CMD ["replica"]

FROM runtime AS api
COPY --from=build-api /api-server /usr/local/bin/api-server
EXPOSE 8080
CMD ["api-server"]
