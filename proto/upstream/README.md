# Vendored enforcer API

These protobuf files are copied from:

- Repository: <https://github.com/LayerTwo-Labs/bip300301_enforcer>
- Commit: `e401e33ba94bc1b94c0ec164712f3dfec9ab70a6`
- Copied: 2026-07-28

Only the read-only `ValidatorService` contract and its direct CUSF message
dependencies are vendored. The monitor does not link to the enforcer
implementation.

## Files and SHA-256

```text
aa6f2f0f2afa1794e98ffecd71466c689b8ede823ecfd4963a04a23598931e80  cusf/common/v1/common.proto
7b9fabbd734dcac30fc76e08ccc286b66828bf739eeb6af7bd5ade818ba899b0  cusf/mainchain/v1/common.proto
9477be451dd643c9ef88314469d0f42891131c804f94527b48638d2691539c76  cusf/mainchain/v1/validator.proto
```

The pinned upstream commit does not contain a root license file. This
provenance note records that fact rather than attributing a license that is not
present upstream. The `bip300-monitor` source outside this directory is
licensed under MIT.

Before updating these files:

1. review the upstream API and commit;
2. replace all three files together;
3. update the commit and checksums above;
4. run the full workspace tests and the real-enforcer probe.

