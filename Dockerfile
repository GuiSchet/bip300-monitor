# syntax=docker/dockerfile:1

ARG RUST_IMAGE=rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc
ARG RUNTIME_IMAGE=debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818

FROM ${RUST_IMAGE} AS builder

ARG CARGO_BUILD_JOBS=2

WORKDIR /src

ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS} \
    CARGO_INCREMENTAL=0

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
      libprotobuf-dev \
      protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY proto ./proto
COPY shared ./shared
COPY extractors ./extractors
COPY tools ./tools

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build \
      --release \
      --locked \
      --package enforcer-extractor \
      --package event-logger \
    && mkdir /out \
    && strip \
      /src/target/release/enforcer-extractor \
      /src/target/release/event-logger \
    && cp \
      /src/target/release/enforcer-extractor \
      /src/target/release/event-logger \
      /out/

FROM ${RUNTIME_IMAGE} AS runtime

ARG VCS_REF=unknown
ARG VERSION=0.1.0

LABEL org.opencontainers.image.source="https://github.com/GuiSchet/bip300-monitor" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="MIT"

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 monitor \
    && useradd \
      --uid 10001 \
      --gid 10001 \
      --no-create-home \
      --home-dir /nonexistent \
      --shell /usr/sbin/nologin \
      monitor

WORKDIR /app
USER 10001:10001
STOPSIGNAL SIGTERM

FROM runtime AS enforcer-extractor

COPY --from=builder --chown=10001:10001 \
    /out/enforcer-extractor \
    /usr/local/bin/enforcer-extractor

ENTRYPOINT ["/usr/local/bin/enforcer-extractor"]

FROM runtime AS event-logger

COPY --from=builder --chown=10001:10001 \
    /out/event-logger \
    /usr/local/bin/event-logger

ENTRYPOINT ["/usr/local/bin/event-logger"]
