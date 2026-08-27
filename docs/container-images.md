# Container images

CI publishes two public images from a single `Dockerfile`, one per binary:

| Binary | Image | Dockerfile stage |
| --- | --- | --- |
| `enforcer-extractor` | `guischet/bip300-enforcer-extractor` | `enforcer-extractor` |
| `event-logger` | `guischet/bip300-event-logger` | `event-logger` |

Both stages share a `runtime` base that installs only `ca-certificates`, creates
an unprivileged `monitor` user, and runs as `10001:10001` with `STOPSIGNAL
SIGTERM`. Each final image contains exactly one binary — the smoke test asserts
that the other one is absent.

Images are `linux/amd64` only. `provenance` and `sbom` are disabled so each tag
resolves to a single image manifest rather than an index; see
[Pinning](#pinning) for what that means when you write a digest into a lock.

## Tags

Tags come from `docker/metadata-action`, with `flavor: latest=false` so nothing
is tagged `latest` by accident:

| Tag | When | Mutable |
| --- | --- | --- |
| `sha-<12 hex>` | every publish | no |
| `main` | push to `main` | yes |
| `X.Y.Z`, `X.Y` | push of a `vX.Y.Z` tag | `X.Y` moves |
| `latest` | push of a `vX.Y.Z` tag | yes |

`sha-<12>` is the only tag that always names one immutable build, so it is the
one to reference from any lock or deployment.

## Publishing

Publishing is deliberately narrow. Pull requests never publish — they build the
image and run the smoke test with `push: false`. A merge publishes only when the
change actually affected image contents (`needs.changes.outputs.image_inputs`),
so a deployment-only or docs-only merge does not produce a new image. Release
tags (`v*.*.*`) always publish.

Docker Hub credentials live in the `DOCKERHUB_TOKEN` repository secret for the
`guischet` account. Secrets are unavailable to pull requests, which is what
makes the "PRs cannot publish" property structural rather than conventional.

## Release order

The Compose file and `VERSIONS.lock` describe one system, so they move together.
Compose says the extractor records to Postgres and refreshes a liveness file;
only an image built from code that does both can honour that. Deploying with a
pin left behind starts containers that ignore the Postgres settings entirely and
never go healthy — which reads as a broken deployment rather than as a forgotten
promotion.

1. Merge to `main`. Pull requests structurally cannot publish, because the Docker
   Hub secret is unavailable to them.
2. CI builds and publishes `sha-<commit>` for both monitor images.
3. Resolve the digests and promote them in `VERSIONS.lock`, together with
   `MONITOR_IMAGE_COMMIT`, as its own commit.
4. Deploy.

`preflight.sh` enforces the order rather than trusting anyone to remember it: it
refuses to start when commits have touched `shared/`, `extractors/`, `tools/` or
`proto/` since the pinned commit. It is deliberately not a CI check — CI runs on
pull requests, which cannot publish, so the same assertion there would fail every
branch that touches the monitor.

## Pinning

Deployments must pin by digest, never by tag. `deployments/ecash/VERSIONS.lock`
records the digest alongside the tag it came from:

```text
ENFORCER_EXTRACTOR_IMAGE=docker.io/guischet/bip300-enforcer-extractor:sha-29406f6ceb28@sha256:944f14...
```

Resolve a digest with:

```bash
docker buildx imagetools inspect docker.io/guischet/bip300-enforcer-extractor:sha-7295bce0e4e8 \
  --format '{{.Manifest.Digest}}'
```

Third-party images (the node, the enforcer, NATS) publish a multi-architecture
index, so their digest is the index digest — the same value `docker pull <tag>`
reports. The monitor images have no index, because CI builds a single platform,
so their tag resolves straight to one manifest. Both forms are immutable.

What matters is that the digest resolves to `linux/amd64`. Pinning another
platform's child digest out of a multi-architecture index resolves fine on any
machine but fails at container start with an exec format error, so
`deployments/ecash/scripts/preflight.sh` checks the resolved platform of every
pinned image.

## Building locally

Always pass `--target`. Omitting it builds whichever stage comes last in the
`Dockerfile`, which happens to be `event-logger` and is not a promise:

```bash
docker build --target event-logger --tag bip300-event-logger:dev .
docker build --target enforcer-extractor --tag bip300-enforcer-extractor:dev .
```

Useful build arguments, all optional:

| Argument | Default | Purpose |
| --- | --- | --- |
| `CARGO_BUILD_JOBS` | `2` | build parallelism |
| `VCS_REF` | `unknown` | `org.opencontainers.image.revision` label |
| `VERSION` | `0.1.0` | `org.opencontainers.image.version` label |
| `RUST_IMAGE` | pinned by digest | build toolchain image |
| `RUNTIME_IMAGE` | pinned by digest | runtime base image |

`RUST_IMAGE` must stay in step with `rust-toolchain.toml`; the `quality` CI job
fails when the two disagree, and the build copies `rust-toolchain.toml` into the
image so rustup resolves the same toolchain CI validated.

Run the same smoke test CI runs:

```bash
scripts/container-smoke.sh bip300-event-logger:dev event-logger enforcer-extractor
```

The arguments are the image reference, the binary that must be present, and the
binary that must be absent.
