# Official enforcer API

- Repository: https://github.com/LayerTwo-Labs/bip300301_enforcer
- Official commit: `1753fc0c23863bcb39c681e1cfaea2705613516f`

Files are copied verbatim from `proto/` at that commit. `SHA256SUMS` records
their bytes. No patch, private LMDB access, or fork-specific RPC is supported.
Run `.github/scripts/check-proto-vendor.sh --online` when updating the pin.

Contract 9 capabilities are documented in `docs/official-sources.md`. The
official enforcer supplies per-block work, no transactional revision, and no
mempool readiness proof. Normalization must not invent these guarantees.
The pinned upstream has no root license file; no upstream license is inferred.
