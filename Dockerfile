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

# The single wdxref binary can run any combination of roles. The per-role targets
# below preserve the existing one-container-per-role deployment model; each just
# selects its role via the CMD. Roles can also be combined in a single container
# by passing multiple roles (or setting ROLES), e.g. CMD ["primary", "replicator", "api"].
FROM runtime AS primary
CMD ["primary"]

FROM runtime AS replicator
# The dedicated replicator image serves the replication endpoints at the legacy
# root paths (/replicate/*) so existing replicas that point UPSTREAM_URL at the
# host root keep working. When the replicator is combined with the api role in a
# single container it defaults to the API namespace (/v1/replicate/*) instead,
# letting both roles share one listen address.
ENV REPLICATE_BASE_PATH=/
CMD ["replicator"]

FROM runtime AS replica
CMD ["replica"]

FROM runtime AS api
CMD ["api"]
