# Vendored enforcer API

These protobuf files are copied from:

- Repository: <https://github.com/LayerTwo-Labs/bip300301_enforcer>
- Commit: `9c056a465c9e940e80d47ccb36fe10c6f3fbcb0d`
- Copied: 2026-09-10

Only the read-only `ValidatorService` contract and its direct CUSF message
dependencies are vendored. The monitor does not link to the enforcer
implementation.

## Files and SHA-256

```text
aa6f2f0f2afa1794e98ffecd71466c689b8ede823ecfd4963a04a23598931e80  cusf/common/v1/common.proto
7b9fabbd734dcac30fc76e08ccc286b66828bf739eeb6af7bd5ade818ba899b0  cusf/mainchain/v1/common.proto
dcf73eaa876416153de8a888acd55aa38f6ba3bff204a2bfa5cb8f583009716b  cusf/mainchain/v1/validator.proto
```

The pinned upstream commit does not contain a root license file. This
provenance note records that fact rather than attributing a license that is not
present upstream. The `bip300-monitor` source outside this directory is
licensed under MIT.

The commit above must equal `ENFORCER_COMMIT` in
`deployments/ecash/VERSIONS.lock`. `.github/scripts/check-proto-vendor.sh`
enforces that, re-checks the checksums below against the files on disk, and in
CI also re-downloads the upstream files to confirm they still match byte for
byte. Promoting the enforcer therefore requires re-vendoring here in the same
change.

Before updating these files:

1. review the upstream API and commit;
2. replace all three files together;
3. update the commit and checksums above;
4. run `.github/scripts/check-proto-vendor.sh --online`;
5. run the CI-equivalent workspace tests and
   `deployments/ecash/scripts/static-check.sh`;
6. after deploying the reviewed commit, run `just verify` and
   `just verify-live` from `deployments/ecash` against the real enforcer.
