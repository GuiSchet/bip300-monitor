# Vendored enforcer API

These protobuf files are copied from the reviewed observer fork:

- Official repository: <https://github.com/LayerTwo-Labs/bip300301_enforcer>
- Observer fork: <https://github.com/GuiSchet/bip300301_enforcer>
- Base commit: `7958ceffa997ffa905f046de71c8e3cf33437c2d`
- Observer commit: `2ea92c062869199bfec21dd21fdd6192a87dafec`
- Local branch: `feature/bip300-monitor-block-delta` in
  `upstream/enforcer`
- Published branch: `feature/bip300-monitor-block-delta` in the observer fork
- Copied: 2026-09-14

Only the read-only `ValidatorService` contract and its direct CUSF message
dependencies are vendored. The monitor does not link to the enforcer
implementation. The observer commit is based directly on the official commit
and adds `GetBip300BlockDelta`; `VERSIONS.lock` pins that exact runtime commit
and its immutable OCI image for the delta-history deployment gate.

`validator-observer.patch` is the reproducible delta from the official
`validator.proto` to the vendored observer contract. This lets CI reconstruct
and verify the contract without trusting a moving branch or requiring the
local fork to have been published first.

## Files and SHA-256

```text
aa6f2f0f2afa1794e98ffecd71466c689b8ede823ecfd4963a04a23598931e80  cusf/common/v1/common.proto
7b9fabbd734dcac30fc76e08ccc286b66828bf739eeb6af7bd5ade818ba899b0  cusf/mainchain/v1/common.proto
a68bb63051eb6f1592ae3ea4f702ffe5da018d43a6732759c91deb77523bea60  cusf/mainchain/v1/validator.proto
```

The pinned upstream commit does not contain a root license file. This
provenance note records that fact rather than attributing a license that is not
present upstream. The `bip300-monitor` source outside this directory is
licensed under MIT.

The base and observer commits above must equal `ENFORCER_BASE_COMMIT` and
`ENFORCER_OBSERVER_COMMIT` in `deployments/ecash/VERSIONS.lock`.
`.github/scripts/check-proto-vendor.sh` enforces that, re-checks the checksums
against disk, downloads the official base, applies the checked-in observer
patch and confirms the reconstructed API byte for byte. If the sibling
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
