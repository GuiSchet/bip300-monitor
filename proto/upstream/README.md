# Vendored enforcer API

These protobuf files are copied from the reviewed observer fork:

- Official repository: <https://github.com/LayerTwo-Labs/bip300301_enforcer>
- Observer fork: <https://github.com/GuiSchet/bip300301_enforcer>
- Base commit: `0e27251ef351a522c72ab9ef079f75e06075390f`
- Observer commit: `0740a39380b39885fe8655f79f78150001d8a15b`
- Local branch: `feature/betanet-monitor-observer` in the sibling enforcer checkout
- Published branch: `feature/betanet-monitor-observer` in the observer fork
- Copied: 2026-09-22

Only the read-only `ValidatorService` contract and its direct CUSF message
dependencies are vendored. The monitor does not link to the enforcer
implementation. The observer commit is based directly on the latest reviewed
upstream commit, retains upstream `GetSeenBmmRequests`, and adds only
`GetBip300BlockDelta`. The Betanet candidate lock pins that exact runtime
commit and, after artifact promotion, its immutable OCI image.

`validator-observer.patch` is the reproducible delta from the official
`validator.proto` to the vendored observer contract. This lets CI reconstruct
and verify the contract without trusting a moving branch or requiring the
local fork to have been published first.

## Files and SHA-256

```text
aa6f2f0f2afa1794e98ffecd71466c689b8ede823ecfd4963a04a23598931e80  cusf/common/v1/common.proto
7b9fabbd734dcac30fc76e08ccc286b66828bf739eeb6af7bd5ade818ba899b0  cusf/mainchain/v1/common.proto
5d0cc889a051fde2d875155cb59161d93d4eff4f9f8554c400dea7d79614671e  cusf/mainchain/v1/validator.proto
```

The pinned upstream commit does not contain a root license file. This
provenance note records that fact rather than attributing a license that is not
present upstream. The `bip300-monitor` source outside this directory is
licensed under MIT.

The base and observer commits above must equal `ENFORCER_BASE_COMMIT` and
`ENFORCER_OBSERVER_COMMIT` in the deployment lock selected through
`BIP300_MONITOR_PROTO_VERSIONS_FILE`. While Betanet is staged, CI selects
`deployments/ecash/VERSIONS.betanet.lock.example`; after promotion the same
values move into `VERSIONS.lock`. `.github/scripts/check-proto-vendor.sh`
enforces that, re-checks the checksums against disk, downloads the official
base, applies the checked-in observer patch and confirms the reconstructed API
byte for byte. If the sibling
`upstream/enforcer` checkout is available, it additionally verifies the exact
observer commit and its parent. A later observer update therefore requires
re-vendoring and changing the immutable runtime pin in the same change.

Before updating these files:

1. review the upstream API and commit;
2. replace all three files and `validator-observer.patch` together;
3. update both commits and the checksums above;
4. run `.github/scripts/check-proto-vendor.sh --online`;
5. run the CI-equivalent workspace tests and
   `deployments/ecash/scripts/static-check.sh`;
6. after deploying the reviewed commit, run `just verify` and
   `just verify-live` from `deployments/ecash` against the real enforcer.
