# syntax=docker/dockerfile:1
# Build context MUST be a pristine checkout of the official locked enforcer.
FROM rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc AS builder
WORKDIR /src
COPY . .
RUN --mount=type=cache,id=official-enforcer-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=official-enforcer-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=official-enforcer-target,target=/src/target \
    cargo build --release --locked --jobs 2 --bin bip300301_enforcer \
    && mkdir /out && cp target/release/bip300301_enforcer /out/ \
    && strip /out/bip300301_enforcer
FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171
ARG ENFORCER_COMMIT
LABEL org.opencontainers.image.source="https://github.com/LayerTwo-Labs/bip300301_enforcer" \
      org.opencontainers.image.revision="${ENFORCER_COMMIT}"
RUN apt-get update && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/bip300301_enforcer /usr/local/bin/
ENTRYPOINT ["bip300301_enforcer"]
