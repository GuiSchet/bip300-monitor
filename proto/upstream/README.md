# Official enforcer API

- Repository: https://github.com/LayerTwo-Labs/bip300301_enforcer
- Official commit: `1753fc0c23863bcb39c681e1cfaea2705613516f`
- Official commit: `d9d9ba89dbd22187c84de2f4347744ea6e07dd4c`
- Official commit: `f13e4e2ae045ebe4d80d35de7216dc1810fc2b9d`

Files are copied verbatim from `proto/` at each official commit listed above;
their bytes are identical at every one of them. `SHA256SUMS` records those
bytes. Each network's lock pins one listed commit, so networks can move to a
new enforcer at different times while the API they share stays the same. Before
listing a commit, run `.github/scripts/check-proto-vendor.sh --online` against a
lock that pins it. No patch, private LMDB access, or fork-specific RPC is
supported.

Contract 9 capabilities are documented in `docs/official-sources.md`. The
official enforcer supplies per-block work, no transactional revision, and no
mempool readiness proof. Normalization must not invent these guarantees.
The pinned upstream has no root license file; no upstream license is inferred.
