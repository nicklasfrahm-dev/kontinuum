# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.26-trixie AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION

WORKDIR /app

ENV GOCACHE=/go-cache
ENV GOMODCACHE=/gomod-cache
ENV CGO_ENABLED=0

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/gomod-cache go mod download

COPY . .

# `make build` — same target local dev and air use — so this image is
# built exactly the way everyone else builds the binary: it also vendors
# Tailwind CSS, HTMX, and PrismJS into pkg/ui/assets/vendor (see the
# `generate` prerequisite in Makefile and pkg/ui/assets.go's go:embed),
# which isn't committed to the repo since the artifact that needs to work
# air-gapped is this image, not the source tree. GOOS/GOARCH cross-compile
# for TARGETPLATFORM; the generate step itself always runs on BUILDPLATFORM
# regardless, since it only downloads static JS/CSS and regenerates
# already-committed CRD/deepcopy files rather than compiling anything.
RUN --mount=type=cache,target=/gomod-cache --mount=type=cache,target=/go-cache \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    make build VERSION=${VERSION} BINDIR=/out

FROM gcr.io/distroless/static-debian13 AS runtime

WORKDIR /app
COPY --from=build /out/kontinuum /app/kontinuum

EXPOSE 8080
ENTRYPOINT ["/app/kontinuum", "serve"]
