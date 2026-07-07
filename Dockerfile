FROM --platform=$BUILDPLATFORM docker.io/golang:1.26.1-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/

FROM builder AS build
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /wdxref ./cmd/wdxref

FROM docker.io/alpine:3.20 AS runtime
COPY --from=build /wdxref /usr/local/bin/wdxref
ENTRYPOINT ["wdxref"]

# The combined wdxref image is the general-purpose entrypoint for running any
# combination of roles in a single container. It ships no default CMD, so the
# operator selects the roles via arguments (e.g. ["primary", "replicator", "api"])
# or the ROLES environment variable. This is the recommended image for new
# multi-role deployments.
FROM runtime AS wdxref
