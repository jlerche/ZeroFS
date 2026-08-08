# Production Copy-on-Write Branches Plan

## Objective

- [ ] Deliver production-ready copy-on-write branches with safe deletion of checkpoints and branches that have descendants.
- [ ] Deliver production garbage collection that eventually reclaims unreferenced shared data without risking deletion of reachable data.
- [ ] Replace the current overgrown implementation with a smaller architecture whose safety follows from explicit storage invariants.
- [ ] Keep the current branch as a research record and source of tests, edge cases, and documentation rather than using it as the production foundation.

## Architectural decisions

- [x] Model every ready branch as an independent durable root over immutable storage objects.
- [x] Treat branch ancestry as historical metadata, not as a storage-liveness dependency.
- [x] Treat named checkpoints, branch heads, and active leases as garbage-collection roots.
- [x] Make namespace deletion immediate and physical reclamation asynchronous.
- [x] Use generation-fenced, streaming mark-and-sweep for shared and inherited data.
- [x] Use fast local garbage collection only when a segment is provably private to one branch.
- [x] Prefer safe retention over deletion whenever metadata is corrupt, unavailable, ambiguous, or concurrently changing.
- [x] Keep the branch feature independent of unrelated 9P, FUSE, TLS, authentication, CSI, frontend, dependency, NBD, and replication redesigns.

## Phase 0: Preserve research and reset scope

### Epic 0.1: Preserve useful work

- [x] Preserve the current branch under a research-oriented name such as `jacob/cow-branches-research`.
  - [x] Record the current commit IDs and dirty working-tree state.
  - [x] Decide whether uncommitted research changes need a separate archival commit or patch bundle.
  - [x] Document that the research branch is not a merge candidate.
- [x] Inventory reusable artifacts from the research branch.
  - [x] Catalogue branch lifecycle tests and the invariant each test exercises.
  - [x] Catalogue crash, retry, stale-record, deletion-race, and garbage-collection scenarios.
  - [x] Preserve useful CLI and RPC naming as an API specification draft.
  - [x] Preserve useful branch and checkpoint documentation as product-behavior input.
  - [x] Preserve stable branch UUID, origin-checkpoint, and catalog-generation concepts.
  - [x] Preserve segment ownership and conservative reclamation ideas that remain compatible with the new design.

### Epic 0.2: Reject accidental scope

- [x] Start a fresh implementation branch from `main`.
- [x] Exclude the current approximately 8,500-line `BranchManager` implementation.
- [x] Exclude activity fences, takeover generations, publication receipts, expiry buckets, deletion plans, and recovery sweeps unless a written invariant proves one is necessary.
- [x] Exclude the candidate-by-candidate global GC implementation that can open tens of thousands of external views per pass.
- [x] Exclude the monolithic registry design in which every mutation rewrites a global JSON object of up to 16 MiB.
- [x] Exclude unrelated changes to the 9P replay model, FUSE lifecycle, transport security, web authentication, CSI productionization, dependencies, replication, and NBD.
- [x] Require every future cross-subsystem change to identify the branch or GC invariant that makes it necessary.

### Epic 0.3: Restore a reviewable baseline

- [x] Confirm the fresh branch passes `cargo check --workspace --all-targets`.
- [ ] Confirm the fresh branch passes the repository's standard formatting, lint, and test gates.
- [x] Avoid carrying over the current `FuseTasks` compilation failure in `zerofs/src/mount.rs`.
- [x] Define a pull-request size and scope policy for the implementation.
  - [x] Keep storage-model, catalog, lifecycle, GC, API, and integration changes in separate reviewable commits or pull requests.
  - [x] Require production code and its focused tests to land together.

## Phase 1: Specify semantics and invariants

### Epic 1.1: Define branch semantics

- [x] Write a concise branch state-machine specification.
  - [x] Define `Creating`, `Ready`, `Deleting`, and `Deleted` or tombstoned states.
  - [x] Define which transitions are externally visible.
  - [x] Define retry and idempotency behavior for every transition.
  - [x] Define recovery behavior after ambiguous process or object-store failures.
- [x] Define a ready branch as independently mountable without its parent branch or source checkpoint.
  - [x] Require the branch's durable root to exist before publishing `Ready`.
  - [x] Require the durable root to pin every immutable object the branch may read.
  - [x] Verify that the SlateDB clone/final-checkpoint mechanism provides this property or add the smallest required pinning layer.
- [x] Define ancestry behavior after deletion.
  - [x] Decide whether `parent_id` continues to reference a tombstoned UUID, becomes optional, or is reparented for display purposes.
  - [x] Ensure ancestry representation does not control physical liveness.
  - [x] Preserve origin metadata needed for audit and user-facing history.

### Epic 1.2: Define deletion semantics

- [x] Allow deletion of a checkpoint referenced as the historical origin of a ready branch.
  - [x] Remove the checkpoint's named GC root.
  - [x] Keep descendant branches valid through their independent durable roots.
  - [x] Serialize checkpoint deletion safely against branch creation that is still in progress.
- [x] Allow deletion of a branch that has descendants.
  - [x] Remove or tombstone the deleted branch's GC root.
  - [x] Keep every descendant independently mountable and writable.
  - [x] Define behavior for active mounts and writers on the deleted branch.
  - [x] Ensure deletion by name cannot affect a subsequently recreated branch with the same name.
- [x] Make logical deletion fast and physical deletion asynchronous.
  - [x] Reject new opens by name after the deletion linearization point.
  - [x] Retain a compact tombstone until old leases and GC generations can no longer observe the former root.
  - [x] Make repeated deletion requests idempotent.

### Epic 1.3: Define GC safety invariants

- [x] Enumerate every authoritative GC root.
  - [x] Live branch heads.
  - [x] Live named checkpoints.
  - [x] Internal durable roots required by ready branches.
  - [x] Active mount or writer leases.
  - [x] Replication or recovery roots that can still expose old data.
  - [x] Roots pinned by an in-progress GC run.
- [x] Define object eligibility for physical deletion.
  - [x] The object is absent from a complete reachable set.
  - [x] The object predates the GC inventory cutoff.
  - [x] The catalog generation and captured roots remain valid.
  - [x] The object remains unreachable after a grace period and second observation.
  - [x] No unreadable, corrupt, or ambiguous metadata was treated as evidence of unreachability.
- [x] Define fail-safe behavior.
  - [x] Interrupted collection leaks storage rather than deleting live data.
  - [x] Catalog changes abort or restart a collection rather than being reconciled speculatively.
  - [x] Delete operations are idempotent.
  - [x] Missing mark data prevents deletion.

## Phase 2: Build the durable catalog and root model

### Epic 2.1: Implement a scalable catalog

- [x] Design a catalog that does not rewrite one multi-megabyte global JSON document for every mutation.
  - [x] Store branch records in independently addressable entries or bounded shards.
  - [x] Store checkpoint records in independently addressable entries or bounded shards.
  - [x] Store tombstones separately from hot branch metadata where appropriate.
  - [x] Bound record sizes and validate all externally supplied names and identifiers.
- [x] Add a monotonically changing catalog generation or equivalent consistent-snapshot token.
  - [x] Change the generation whenever the set or identity of GC roots changes.
  - [x] Make a root visible only after its durable storage state exists.
  - [x] Make root removal and tombstone publication atomic from the catalog reader's perspective.
- [x] Define catalog consistency and contention behavior.
  - [x] Avoid one CAS domain for unrelated mounts, checkpoint fences, and branch updates.
  - [x] Bound retries and return actionable contention errors.
  - [ ] Test concurrent creates, deletes, renames if supported, mounts, and checkpoint operations.

### Epic 2.2: Implement durable branch roots

- [x] Create a branch from an exact immutable checkpoint identity.
  - [x] Resolve checkpoint name to stable UUID and manifest identity once.
  - [x] Prevent deletion or replacement of that exact source while clone publication is incomplete.
  - [x] Create the destination's independent durable root.
  - [x] Publish the branch as `Ready` only after the root is durable.
- [ ] Support creation from live head only if it can be reduced to the same checkpoint-based primitive.
  - [ ] Flush the parent to a durable point.
  - [ ] Create a temporary internal checkpoint with a stable identity.
  - [ ] Clone from that checkpoint.
  - [ ] Remove the temporary public dependency after the destination root is independently pinned.
- [ ] Keep the recovery record minimal.
  - [x] Persist an operation ID and immutable source/destination identities.
  - [ ] Resume or safely roll back an incomplete create.
  - [x] Avoid per-step receipts when the operation can be reconciled from authoritative state.

### Epic 2.3: Implement leases

- [x] Define bounded leases for active mounts and writers.
  - [x] Bind each lease to an exact branch UUID and root identity rather than only a reusable name.
  - [x] Define renewal, expiry, shutdown, and crash behavior.
  - [x] Ensure an expired lease cannot be resurrected accidentally.
- [x] Make active leases explicit GC roots.
- [x] Allow logical deletion while preserving data required by unexpired leases according to documented semantics.
- [ ] Test deletion, remount, process crash, lease expiry, and name reuse races.

## Phase 3: Implement the branch lifecycle

### Epic 3.1: Create and inspect branches

- [x] Implement branch creation from a named checkpoint.
  - [x] Validate branch names and reject reserved names.
  - [x] Use stable UUIDs for branches and operations.
  - [x] Make exact retries idempotent.
  - [x] Reject conflicting retries with clear diagnostics.
- [ ] Implement branch listing and inspection.
  - [ ] Report UUID, state, current root, origin, and historical parent.
  - [ ] Distinguish live parents/checkpoints from tombstoned historical origins.
- [ ] Implement branch mounting by stable identity resolved from a name.
  - [ ] Acquire a lease before exposing the data plane.
  - [ ] Verify the branch is `Ready` and the exact root is readable.

### Epic 3.2: Delete checkpoints independently

- [x] Implement logical checkpoint deletion.
  - [x] Fence only branch creations that have not yet established independent durable roots.
  - [x] Do not block deletion because a ready branch records the checkpoint as historical provenance.
  - [x] Preserve ready descendant branches.
- [x] Add race tests.
  - [x] Delete while branch creation has not started cloning.
  - [x] Delete while clone storage exists but the branch is not yet published.
  - [x] Delete after the branch is ready.
  - [x] Retry after an ambiguous deletion response.

### Epic 3.3: Delete branches with descendants

- [x] Implement logical branch deletion without recursively deleting descendants.
  - [x] Tombstone the exact branch UUID.
  - [x] Preserve descendants' roots, mountability, and write behavior.
  - [x] Preserve enough historical metadata to explain lineage.
  - [x] Prevent stale requests from deleting a new branch that reused the old name.
- [x] Add descendant tests.
  - [x] Delete a parent with one child.
  - [x] Delete a middle branch in a deep lineage.
  - [x] Delete ancestors in different orders.
  - [x] Delete a branch while descendants are mounted or being created.
  - [x] Recreate a deleted branch name and verify identity isolation.

## Phase 4: Implement production garbage collection

### Epic 4.1: Build a root-capture protocol

- [x] Begin each global GC run by reading catalog generation `G`.
- [x] Capture exact immutable root identities for all live branches, checkpoints, and leases.
- [x] Pin the captured roots for the duration of the run.
- [x] Record a physical-inventory cutoff so newer objects are never eligible in that run.
- [x] Persist a compact resumable run record.
  - [x] Run UUID.
  - [x] Catalog generation.
  - [x] Inventory cutoff.
  - [x] Captured root identities or a digest plus immutable root-list object.
  - [x] Mark-shard locations.
  - [x] Current phase.
  - [x] Quarantine timestamp.
- [x] Abort safely when a root cannot be opened, authenticated, or enumerated.

### Epic 4.2: Stream the reachable set

- [x] Enumerate segment references from each pinned root exactly once per run.
- [x] Avoid `candidate count × branch count × checkpoint count` point-read behavior.
- [x] Emit reachable segment IDs into bounded sorted runs.
  - [x] Partition runs by a stable segment-ID prefix.
  - [x] Bound memory independently of total live storage.
  - [x] Persist checksums and run metadata.
- [x] Merge and deduplicate sorted runs into authoritative mark shards.
  - [x] Make every shard independently verifiable.
  - [x] Treat missing or corrupt shards as a failed run.
  - [x] Permit Bloom filters only as performance hints, never as deletion authority.
- [x] Measure work proportional to reachable references rather than repeated remote view probes.

### Epic 4.3: Inventory and quarantine unreachable objects

- [x] Stream the physical segment inventory by the same stable shard key.
- [x] Join each inventory shard against its authoritative mark shard.
- [x] Exclude objects newer than the inventory cutoff.
- [x] Write unreachable candidates to a durable quarantine set.
- [x] Do not physically delete during the first unreachable observation.
- [x] Record reasons that prevent deletion, including missing roots, corrupt metadata, generation changes, and lease uncertainty.

### Epic 4.4: Revalidate and delete

- [x] Re-read the catalog after marking and require generation `G` before accepting the run.
- [x] Wait a configurable grace period that exceeds relevant lease, propagation, and clock-skew bounds.
- [x] Perform a second independent reachability observation or equivalent generation-fenced validation.
- [x] Remove candidates that became reachable or cannot be proven unreachable.
- [x] Delete remaining objects in bounded idempotent batches.
- [x] Persist batch progress so crashes resume safely.
- [x] Retain deletion audit metrics without retaining unbounded per-object metadata.

### Epic 4.5: Add fast local GC

- [x] Define a proof that a segment is private to one exact branch incarnation.
- [ ] Reclaim private segments without consulting every other branch or checkpoint.
- [ ] Continue using global GC for inherited, shared, or ambiguous segments.
- [ ] Ensure local GC respects checkpoints and active leases on the same branch.
- [ ] Fall back to global retention whenever private ownership cannot be proven.

### Epic 4.6: Clean up metadata

- [ ] Remove old tombstones only after no active lease or GC run can observe their catalog generation.
- [ ] Remove completed GC run artifacts after a retention period.
- [ ] Remove obsolete mark runs and quarantine records idempotently.
- [ ] Bound cleanup work per pass and expose backlog metrics.

## Phase 5: Verification and fault testing

### Epic 5.1: Validate branch lifecycle safety

- [ ] Port only research-branch tests that correspond to claims made by the new design.
- [ ] Test process crashes before and after every lifecycle linearization point.
- [ ] Test ambiguous object-store success and retry behavior.
- [ ] Test stale clients, duplicate operation IDs, and conflicting operation IDs.
- [ ] Test checkpoint and branch name reuse with stable UUID isolation.
- [ ] Test deep lineages while avoiding a runtime dependency on ancestor availability.

### Epic 5.2: Validate GC safety

- [ ] Build a model test that computes ideal reachability and compares it with collector decisions.
- [ ] Test catalog changes during root capture, marking, inventory, quarantine, and deletion.
- [ ] Test branches and checkpoints created immediately before and after the inventory cutoff.
- [ ] Test deletion of parents and source checkpoints with surviving descendants.
- [ ] Test active, expired, renewed, and corrupted leases.
- [ ] Test missing, corrupt, truncated, duplicated, and reordered mark shards.
- [ ] Test collector crashes and restarts in every persisted phase.
- [ ] Test partial and ambiguous object-store deletes.
- [ ] Assert that uncertainty always retains data.
- [ ] Assert eventual reclamation once an object is stably unreachable.

### Epic 5.3: Validate scale and operability

- [ ] Establish supported production limits for branches, checkpoints, lineage depth, leases, and segment inventory.
- [ ] Benchmark root capture and mark generation at those limits.
- [ ] Verify memory use is bounded by run/shard size rather than total storage size.
- [ ] Verify external work scales linearly with roots, references, and inventory.
- [ ] Verify foreground branch and mount latency remains acceptable during GC.
- [ ] Verify catalog mutations do not contend on one global multi-megabyte CAS object.
- [ ] Add metrics for phase duration, scanned references, inventory size, quarantined bytes, reclaimed bytes, aborted runs, retained-on-error objects, and backlog.
- [ ] Add alerts for repeated aborted runs, stalled phases, old quarantines, root-open failures, and catalog corruption.

## Phase 6: Production rollout

### Epic 6.1: Ship lifecycle behavior safely

- [ ] Release create, list, inspect, mount, checkpoint deletion, and descendant-preserving branch deletion behind feature controls where appropriate.
- [ ] Provide a reviewed offline legacy-to-pool migration that preserves the exact volume key, reserves every imported epoch, rejects duplicate physical segment IDs across sources, and publishes authoritative completion; alternatively rewrite every colliding segment ID and `FrameLoc`.
- [ ] Document exact semantics for logical deletion, active mounts, tombstones, name reuse, and asynchronous reclamation.
- [ ] Provide administrative inspection for branch UUIDs, durable roots, leases, tombstones, and incomplete operations.
- [ ] Provide bounded repair or cleanup operations for states that cannot recover automatically.

### Epic 6.2: Shadow GC

- [ ] Run global GC in mark-only reporting mode.
- [ ] Compare proposed decisions with the existing collector and an offline ideal-reachability calculation.
- [ ] Investigate every disagreement before enabling quarantine.
- [ ] Record expected reclaimable bytes and completion time under representative workloads.

### Epic 6.3: Quarantine rollout

- [ ] Enable durable quarantine without physical deletion.
- [ ] Observe multiple complete GC cycles and catalog mutations.
- [ ] Confirm quarantined objects remain absent from every valid root across the grace period.
- [ ] Exercise recovery from forced collector and object-store failures.

### Epic 6.4: Physical deletion rollout

- [ ] Enable deletion behind a feature flag with conservative age and grace thresholds.
- [ ] Start with bounded canary deployments and small delete batches.
- [ ] Verify restore, audit, and incident-response procedures before broader rollout.
- [ ] Gradually increase throughput only after safety and foreground-latency targets hold.
- [ ] Keep a rapid kill switch that disables physical deletion while allowing marking and reporting to continue.

## Phase 7: Completion criteria

### Epic 7.1: Functional acceptance

- [ ] A ready branch remains readable and writable after deletion of its source checkpoint.
- [ ] A descendant remains readable and writable after deletion of any or all logical ancestors.
- [ ] Branch and checkpoint operations are idempotent across retries and process crashes.
- [ ] Name reuse cannot confuse identities or delete the wrong incarnation.
- [ ] Existing mounts follow the documented lease and deletion behavior.

### Epic 7.2: GC acceptance

- [ ] Every physically deleted segment was absent from all authoritative roots in two generation-fenced observations separated by the grace period.
- [ ] Objects created after a run's cutoff cannot be deleted by that run.
- [ ] Corrupt, missing, or unreadable metadata always prevents affected deletion.
- [ ] Shared objects are eventually reclaimed after their final root disappears.
- [ ] GC work is streamable and bounded and does not perform candidate-by-candidate scans across every branch and checkpoint.
- [ ] Interrupted GC runs resume or abort without unsafe partial effects.

### Epic 7.3: Engineering acceptance

- [ ] The workspace compiles and passes formatting, linting, unit, integration, fault-injection, and model-test gates.
- [ ] The implementation is split into coherent, reviewable subsystems.
- [ ] Production behavior is documented with operational limits and failure semantics.
- [ ] Metrics, alerts, administrative inspection, rollout controls, and a deletion kill switch are available.
- [ ] The research branch remains reference material and is not merged wholesale.

## Review findings that motivated this plan

- [ ] The committed research branch added approximately 20,237 lines across three commits.
- [ ] The reviewed dirty tree expanded the overall tracked diff to approximately 36,823 additions and 12,346 deletions, plus untracked files.
- [ ] The three central branch files contained approximately 13,683 lines, of which roughly 5,800 were tests.
- [ ] The implementation expanded into a bespoke distributed transaction protocol rather than relying on a small set of durable storage invariants.
- [ ] The GC design allowed up to 49,152 external views and 262,144 work units per pass.
- [ ] The registry placed branch topology, mounts, and checkpoint fences in one global CAS document of up to 16 MiB.
- [ ] The dirty tree did not pass `cargo check --workspace --all-targets` because of a `FuseTasks` field error.
- [ ] The feature accumulated broad unrelated changes that made correctness, security, and performance review impractical.
- [ ] The useful output of the research branch is its catalogue of races, tests, product semantics, and failure cases—not its implementation trajectory.

## Implementation record

### 2026-08-08: baseline and catalog slice

- Fork synchronization: `main`, `upstream/main`, and `origin/main` were fast-forwarded to `26769980a5fd96fc99b30932f725631d733c1750`.
- Research source: `jacob/cow-branches-poc-research` is clean at archival commit `f14a299` (`0b411a6` plus the complete formerly dirty worktree). It is reference material, not a merge candidate, and has not been pushed.
- Production branch: `jacob/cow-branches-production` was created from synchronized `main`. The first slice contains only the catalog contracts, authoritative SlateDB backend, customer projections, schema, validation, and focused tests.
- Storage authority: SlateDB is mandatory locally and in production. It stores stable branch/checkpoint UUIDs, lifecycle state, durable root and manifest identities, ancestry, generation, name indexes, and tombstones as independent keys updated in atomic durable batches.
- Projection selection: JSON and PostgreSQL implement the same reconstructible customer-facing projection. JSON defaults to `.zerofs/catalog-projection.json` for local development/tests; production selects PostgreSQL. PostgreSQL connections force certificate-verified TLS with the platform trust store, while externally managed callers may inject a `tokio_postgres::Client`.
- Consistency contract: every successful mutation advances the root-snapshot generation, but unrelated writers do not compare-and-swap on that global value. Updates and deletes use per-record revisions and exact UUIDs, returning actionable revision conflicts without hidden retry loops.
- Projection boundary: PostgreSQL/JSON contain only volume/resource UUIDs, kind, name, customer-visible state, lineage UUIDs, timestamps, observed generation, and customer metadata. They never contain durable roots or manifests and are never consulted for mounting or garbage collection.
- Reconciliation: a projection consumes an authoritative generation-tagged SlateDB snapshot idempotently. Projection outages do not invalidate storage operations; a later reconciliation catches up while preserving customer-managed metadata.
- Independent review gate: review of `31efd05` found cross-kind/tombstone UUID reuse, global-generation contention, projection parity, deleted-lineage reconstruction, identifier-bound, and schema-upgrade issues. The follow-up correction globally reserves UUIDs, uses per-record revisions, retains root-free historical lineage in tombstones, aligns JSON/PostgreSQL behavior, adds bounded crash-resumable SlateDB migrations through the current v5 deletion schema, and adds adversarial tests.

### 2026-08-08: semantics and research inventory

- Normative contract: `docs/cow-branches-semantics.md` defines stable identity/name reuse, the branch state machine, publication and deletion linearization points, exact retry behavior, minimal create recovery, bounded leases, authoritative GC roots, two-observation deletion eligibility, and fail-closed behavior.
- Research reuse: the contract maps research registry, manager, mount, GC, CLI/RPC, and documentation artifacts to specific reusable scenarios while explicitly rejecting the monolithic registry, broad fencing, receipt, takeover, and candidate-by-view designs.
- Review policy: storage, catalog, lifecycle, lease, GC, API, and rollout work remain separate coherent changes. Production code and focused tests land together, and each subtask requires an independent pre-commit review.

### 2026-08-08: durable SlateDB root adapter

- Native mechanism finding: SlateDB clones remain shallow while their manifests reference external SSTs. Clone creation installs unnamed final checkpoints in the physical source namespaces; deleting a customer-named source checkpoint is safe, while deleting those internal pins is not.
- Root representation: `SlateDbRootStore` derives a destination below a configured private branch prefix, creates one operation-scoped internal checkpoint, and records its exact checkpoint/manifest identity as the branch root. Verification requires the canonical immutable operation result, a fully flushed manifest with no outstanding WAL range, final-pin manifest coverage, and existence of every SST resolved across unsegmented and segmented trees without consulting catalog ancestry or customer projections.
- Retry boundary: immutable owner and result descriptors in the private destination namespace are the one storage proof rooted by the matching catalog operation/live root. They bind exact operation/source/destination inputs, reconcile applied writes with lost responses, and elect one canonical root. A crash leaving duplicate operation checkpoints is recovered by deterministic election and best-effort loser cleanup. Descriptors never publish lifecycle state; the authoritative catalog remains the only `Ready` authority.
- Cleanup boundary: the proof descriptors, canonical checkpoint, and destination namespace remain root metadata while referenced by an incomplete operation, live branch, or lease. Noncanonical checkpoints are cleaned on retries; final cleanup waits for exact tombstone/abort state, lease drain, and generation-fenced GC. Uncertainty retains.
- Executable proof: focused in-memory tests clone from an exact checkpoint identity, reject mismatched manifests and WAL-dependent roots before clone I/O, reject unowned, overlapping, oversized, or malformed destinations, converge concurrent retries, recover duplicate checkpoints after every creator crashes before result publication, reject noncanonical losing roots, detect missing reachable SSTs in ordinary and segmented manifests, reconcile injected lost responses, delete the named source checkpoint, read and write the child independently, verify the parent remains unchanged, and fail closed after removal of an external final pin.

### 2026-08-08: authoritative branch-create publication

- Catalog lifecycle: schema v3 adds independently keyed permanent create-operation records with only `reserved`, `root_created`, and `published` phases. Reserved operations root the exact source; root-created operations root only the independently pinned destination; published records enumerate no GC roots and remain only as operation-UUID/idempotency reservations.
- Atomic boundaries: reservation creates the `Creating` branch, name index, operation, and exact source-hold index in one durable batch. Root recording atomically replaces the source hold with the authenticated destination as an incomplete root. Publication re-authenticates storage and atomically changes the branch to `Ready`, retains the exact destination head, and marks the operation published. Raw catalog mutations are crate-private so external callers cannot bypass the storage-verifying lifecycle coordinator; its factory rejects catalog/branch namespace overlap before storage I/O.
- Retry and race proof: the safe name-based entry point resolves once to the catalog checkpoint UUID and encoded SlateDB checkpoint/manifest identity. Exact retries return the existing generation/result, changed immutable inputs or roots conflict, checkpoint deletion and reservation serialize under the exact source hold, snapshots fail closed if that derived hold index diverges from incomplete operations, completed retries do not require a deleted historical source, and generic mutations cannot create or rewrite `Creating` lifecycle state.
- Projection boundary: create-operation phases, source holds, and both durable roots exist only in authoritative SlateDB. JSON and PostgreSQL continue to receive the same root-free branch/checkpoint/tombstone projection.

### 2026-08-08: bounded authoritative leases

- Lease authority: catalog schema v4 stores leases and permanent lease-UUID tombstones only in SlateDB. Each lease binds an exact subject kind/UUID/root, read/write mode, revision, issuance/renewal/expiry times, and SHA-256 renewal-token binding; PostgreSQL and JSON remain unchanged and root-free.
- Acquisition and renewal: the public coordinator authenticates storage before an atomic exact-resource revision/root/state check and lease insertion. Name lookup is only the first locator and the request carries the stable subject UUID. Renewal requires the exact UUID, token, expected revision, unexpired lease, unchanged root, and still-mountable subject. The expected revision plus requested duration is the scoped idempotency key: exact retries reconcile an applied batch with a lost response, while timestamps and expiry can only move forward.
- Expiry and GC: leases are bounded to five minutes. Release is exact and idempotent; crash cleanup expires only after the lease deadline plus 30 seconds of conservative clock skew. Live lease roots participate directly in generation-tagged root snapshots, survive logical subject deletion, and cannot be renewed after deletion or resurrected after expiry/tombstoning.

### 2026-08-08: independent checkpoint deletion

- Exact logical delete: the public deletion coordinator accepts the stable checkpoint UUID, expected revision, and historical name; server time supplies the deletion timestamp. One durable catalog batch removes the live name/root and writes the root-free tombstone with the consumed revision. Exact retries—including a lost success response after name reuse—must match the old UUID/name/revision tuple.
- Narrow fence: deletion conflicts with the exact source-hold index only in `reserved`. Recording the authenticated independent destination root atomically removes that source hold, so a `root_created` operation retains only its destination and can resume/publicize after source deletion. Ready descendants and read leases retain independent roots and never block the logical deletion.
- Race proof: tests cover delete-before-reservation serialization, deletion after destination storage exists but before publication, deletion after ready publication with the descendant still readable, checkpoint lease acquisition versus deletion, and response-loss/name-reuse reconciliation.

### 2026-08-08: descendant-preserving branch deletion

- Two-phase delete: catalog schema v6 adds a permanent, independently keyed branch-delete operation. Its first durable batch changes the exact branch UUID from `Ready` to `Deleting`, increments its revision, and removes only its name index, immediately fencing new mounts and writes without touching checkpoints or descendants. A second batch removes the live branch/root, writes a root-free historical tombstone bound to the operation UUID and consumed branch revision, and marks the operation published for exact crash retries.
- Lease boundary: finalization conservatively waits for every authoritative writer-lease record on the deleted branch to be explicitly released or expired. Reader leases do not block and retain their exact roots independently; no lease can be acquired or renewed after the `Deleting` fence. A draining operation itself roots the branch until finalization, while a published operation retains metadata but contributes no GC root.
- Descendant independence: deletion never traverses lineage. Ready descendant roots, live descendant leases, and reserved/root-created descendant operations remain authoritative in SlateDB and can continue through publication after an ancestor is tombstoned. Tombstones preserve only root-free lineage UUIDs for explanation and the identical PostgreSQL/JSON customer projection.
- Race proof: tests delete parents and middle branches in deep lineages, delete parent/child pairs in both orders, retain mounted descendants, finish a reserved descendant after ancestor deletion, serialize concurrent exact deletion retries, drain an active writer, permit readers, reuse deleted names under new UUIDs, and prove stale retries return only the old incarnation's tombstone.

### 2026-08-08: generation-fenced GC root capture

- Capture boundary: catalog schema v7 stores compact GC run records in authoritative SlateDB only. A run binds its permanent UUID to catalog generation `G`, a server-recorded physical-inventory cutoff, a canonical typed and deduplicated root list, its SHA-256 digest, phase, future mark-shard locations, and quarantine timestamp. PostgreSQL/JSON projections remain identical and contain none of this storage authority.
- Complete pins: capture includes ready/deleting branch roots, checkpoints, incomplete create-operation roots, leases, and roots retained by overlapping active GC runs. Captured run roots participate directly in every later authoritative root snapshot until that run reaches a terminal phase.
- Fail-closed fence: the coordinator authenticates and enumerates every branch root and exact checkpoint root with bounded concurrency before persistence. The durable insert succeeds only if the catalog is still at `G`; any root failure or generation change leaves no partial run. Because each new pin duplicates a root already present at `G`, inserting the run record itself is generation-neutral, while exact retries reconcile by run UUID and immutable contents. Schema v7 accepts only revision-one `captured` runs and retains every stored run's roots; terminal phases cannot be persisted or release pins until a separately reviewed transition/proof protocol lands.
- Executable proof: tests cover typed canonical root collection, durable generation-neutral empty capture and exact retry, mismatched-digest rejection before persistence, fabricated terminal-phase fail-closed retention, unreadable root failure with no run record, and a concurrent catalog mutation rejecting capture without partial pins. Schema migration now accepts every prior catalog through v6 before installing the v7 marker.

### 2026-08-08: streaming authoritative GC marks

- Exact enumeration: each captured durable root is opened through SlateDB's exact checkpoint reader and its extent keyspace is scanned once. Extent values decode directly to segment IDs; malformed values, unreadable roots, and missing checkpoint state fail the run without publishing mark authority. Later writes outside the captured checkpoint are excluded by construction.
- Bounded marking: references accumulate in an 8,192-entry global buffer, partitioned by the low byte of the stable segment counter. Every flush sorts and deduplicates each nonempty partition into a deterministic intermediate object. Per-shard online binary-carry merges retain only logarithmically many object paths, and every merge uses at most two bounded readers and one bounded writer; segment data memory stays fixed independently of live storage and work is driven by captured references instead of candidate-by-root probes.
- Verifiable authority: schema v8 publishes exactly 256 ordered final shards in the authoritative SlateDB run record. Each binary shard binds its format version, run UUID, root digest, shard number, sorted segment records, record count, and SHA-256 checksum. The coordinator verifies every final shard before the atomic `captured` to `marking` transition and re-verifies published shards on retries; missing, truncated, corrupt, reordered, duplicated, or descriptor-mismatched data fails closed. Bloom filters are not deletion authority.
- Work accounting: the run persists root, reference, intermediate-run, and unique-segment counts. Validation ties those statistics to the immutable root list and final shard descriptors, while PostgreSQL and JSON remain identical root-free customer projections.
- Executable proof: the focused test scans 9,000 checkpoint references through two bounded flushes, deduplicates them to 1,000 segments across 256 independently verifiable shards, excludes a post-checkpoint segment, preserves the catalog generation during mark publication, reconciles an exact retry, and rejects a subsequently corrupted authoritative shard. A million-flush stress test proves resident spill-path handles equal the binary population count and never exceed the logarithmic bound.

### 2026-08-08: physical inventory and durable quarantine

- Segment-pool identity: storage configuration names one volume-wide immutable segment pool, disjoint from the authoritative catalog and branch-database namespaces. Runtime readers, writers, replication replay, and GC use that same prefix. An HMAC-authenticated, conditional-create genesis binds the empty pool to its volume key and permanent pool UUID. Every read-write admission reserves a pool-global epoch with an immutable, genesis-authenticated conditional-create marker before counter zero, so independently cloned SlateDB writer epochs cannot collide. Every new GC run stores the canonical pool identity in SlateDB; pre-v9 active runs without it remain pinned and cannot inventory. PostgreSQL and JSON remain identical root-free projections and receive neither pool identity nor GC state.
- Migration fence: the shared pool is also the volume encryption-key root because inherited external SSTs and segment frames must remain decryptable. This slice admits new CoW volumes only. Startup rejects any database with a legacy wrapped key or remaining per-database segment, refuses to establish genesis in a prepopulated pool, and independently proves that every segment already in an admitted pool has a valid permanent marker authenticated for that exact genesis and epoch. Copying or synthesizing shaped key, marker, or segment objects alone cannot manufacture authenticated admission. Legacy single-database mode remains available while the rollout phase designs an authoritative manifest that reserves every old epoch and rejects duplicate segment IDs, or rewrites all colliding IDs and `FrameLoc`s offline.
- Order-independent inventory: schema v9 lists each of the same 256 physical segment prefixes without assuming backend LIST order. It validates every object key and shard, excludes immutable objects newer than the captured cutoff, emits bounded 8,192-object sorted runs, and uses online binary-carry plus two-reader streaming merges so data memory remains fixed and spill-path bookkeeping logarithmic.
- First observation: each sorted inventory shard is merge-joined once against its independently verified authoritative mark shard. Reachable objects are counted, while older unmarked objects are written—with size and immutable modification time—to exactly 256 sorted, checksummed quarantine shards bound to the run UUID and root digest. No physical segment delete exists on this transition.
- Acceptance fence: the `marking` to `quarantined` revision-three publication is one durable SlateDB write and succeeds only while the catalog remains at captured generation `G`. Exact retries verify both mark and quarantine artifacts. Missing roots, corrupt metadata/artifacts, generation changes, lease uncertainty, and storage unavailability have bounded per-kind durable blocker records in SlateDB; they never enter customer projections or authorize deletion.
- Executable proof: the focused lifecycle test inventories 10,001 physical objects, excludes one post-cutoff object, classifies 1,000 reachable segments, externally sorts and quarantines 9,000 one-byte candidates across 256 verifiable shards, proves the candidate still exists, reconciles an exact retry, and records corrupt-artifact blockers. A catalog test proves a generation change rejects quarantine publication and that all blocker categories persist without changing the run revision. Runtime tests reserve 256 unique pool epochs concurrently, read a shallow-cloned extent through the shared pool, reject pre-genesis segments and wrong-key genesis, reject unmarked or forged pool epochs, reject an exact manual legacy-key copy, and prove two legacy databases with the same segment ID cannot join one pool.

### 2026-08-08: generation-fenced GC revalidation

- Grace fence: catalog schema v10 records the configured whole-second grace and its exact `quarantine_at + grace` boundary. Revalidation is rejected before 390 seconds, covering the five-minute maximum lease, 30-second conservative skew, and one-minute propagation allowance. Waiting is caller-scheduled; the lifecycle never blocks a worker by sleeping.
- Independent observation: after the boundary, the coordinator captures and authenticates a fresh complete root set at generation `H`, durably pins it in a generation-neutral revision-four record, and independently rebuilds all 256 mark shards under a new observation UUID and root digest. Both the capture and the final revision-five publication require the catalog still to equal `H`; generation uncertainty retains every candidate.
- Candidate proof: each checksummed first-observation candidate shard is merge-joined with the fresh mark shard. Artifact format v2 streams every first-pass candidate body into a portable SHA-256 identity alongside its size and modification timestamp. Newly reachable candidates are removed. Every still-unmarked object is streamed again at its canonical shared-pool key and retained only if its byte digest and metadata exactly match the first observation; absent objects are recorded as already gone, while changed or unreadable data aborts. The surviving 256 shards and bounded aggregate metrics are verified before atomic publication and must classify exactly the original candidate total. This transition performs no physical deletes and both observations' roots remain authoritative pins.
- Executable proof: focused lifecycle coverage rejects early revalidation, introduces a candidate into a newly pinned independent checkpoint before the second mark, removes another candidate externally, retains a third unchanged candidate without deleting it, verifies exact retry behavior, and checks the reachable/absent/retained accounting. PostgreSQL and JSON remain identical root-free projections and contain no GC state.

### 2026-08-08: bounded physical GC deletion

- Explicit enablement: physical deletion remains disabled by default and requires an explicit per-invocation policy. Batch size is immutable once a run enters deletion, must be between one and 4,096 objects, and every call performs at most one batch before returning durable authority to the caller.
- Durable cursor: catalog schema v11 adds only a shard/record cursor, fixed batch size, start/completion times, and aggregate deleted-object/deleted-byte/already-absent counters. Validation derives the exact processed count from the 256 candidate descriptors and requires it to equal the aggregate classification, so progress cannot skip candidates. Each progress write is revision-checked and generation-fenced to the second observation; exact retries reconcile an applied write with a lost response.
- Idempotent delete: immediately before each delete, the worker streams the canonical pool object and requires the exact v2 SHA-256, size, and timestamp from revalidation. Changed or unreadable content aborts. A successful delete is confirmed absent before the cursor advances. A crash after object deletion but before progress publication safely replays that object as already absent, then persists the same next cursor. Completed runs release their root pins but retain bounded audit totals; artifact cleanup remains the separate Epic 4.6 retention task.
- Forward closure and immutable keys: production catalog code exposes no generic branch/checkpoint root insertion or root replacement; only authenticated lifecycle transitions may publish roots, and unavailable root transitions fail closed until they receive an equivalent lifecycle. Every normal segment writer uses conditional create. Multipart seals stage under unique non-authoritative keys and copy-if-absent into `segments/`; exact collisions reconcile by streaming byte equality, while different bytes fail closed. Consequently an admitted writer cannot replace a checked candidate before deletion or manufacture an old unreachable reference after the generation preflight.
- Single deletion authority: configuring the volume-wide segment pool disables the legacy per-database segment reclaimer, compaction-source deletion, and orphan sweep because their liveness proof covers only one branch database. Metadata/tombstone GC remains active. Until Epic 4.5 supplies an exact private-ownership proof, all shared-pool physical deletion goes through the authoritative global protocol and its default-off policy.
- Executable proof: focused tests delete two candidates with batch size one, observe a durable intermediate `deleting` revision, reconcile an exact lost progress response, resume to `completed`, verify both physical objects are absent, and prove a replay of an uncheckpointed physical delete advances as already absent rather than failing or double-deleting. Additional tests reject test-fixture root replacement, prove exact segment-put retries are idempotent, race two different payloads for one immutable key, cover the multipart copy-if-absent path, and prove branch A's disabled local collector cannot remove a shared segment referenced only by branch B.

### 2026-08-08: fast local GC proof boundary

- Privacy is a capability, not ancestry: a branch UUID on a segment identifies its allocator but does not exclude checkpoint or descendant references. Local eligibility requires an authenticated pool epoch bound to the exact never-reused branch UUID plus a matching authoritative SlateDB private-epoch record.
- Sealing and exposure fences: local GC targets only a `sealed_private` epoch. Sealing first rotates the writer, then holds the branch-local FrameLoc/reference-publication barrier while draining allocation, seal, replication, recovery, compaction, and other publishers; no later local reference may enter that epoch. An atomic durable, non-expiring per-epoch batch guard serializes local deletion against clone-source capture, externally durable checkpoint publication, branch deletion, and lease/recovery-root acquisition. Root publication permanently changes affected `open`/`sealed_private` epochs to `exposed` before source capture; reopening or reusing them is forbidden.
- Irreversible window: each deletion holds the same local reference-publication barrier continuously from final non-reference/guard validation through exact identity validation and confirmed absence. Crash recovery keeps the batch guard, fences the former database writer, and proves its publisher/object requests quiescent before exact resume; the guard cannot expire, be stolen/cleared for age, or permit exposure while an old delete may still complete. Unprovable quiescence blocks exposure and retains data for repair.
- Conservative scope: local candidates must carry the guarded epoch and be absent from every relevant local checkpoint and exact-branch lease/recovery view. Inherited, legacy, exposed, missing, corrupt, or ambiguous ownership always falls back to the global two-observation collector. The existing shared-pool local reclaimer remains disabled until these catalog, writer-rotation, and bounded-progress mechanisms are implemented and independently reviewed.

### 2026-08-08: authenticated branch epoch primitive

- Reservation format v2 extends the permanent pool-global epoch marker with an optional branch UUID covered by the volume-key HMAC. `reserve_branch_epoch` rejects nil identities and binds the exact never-reused branch incarnation alongside the pool, epoch, reservation UUID, and database identity. Changing or stripping the owner invalidates storage admission.
- Compatibility and enablement: authenticated v1 markers and ownerless v2 markers remain accepted for pool uniqueness but carry no private-ownership proof. The normal server path remains ownerless/global-only, authoritative `sealed_private`/`exposed` catalog state is not yet present, and shared-pool local reclamation therefore remains disabled.
- Executable proof: focused tests accept an exact branch-bound marker, reject the same marker after its branch UUID is changed, retain authenticated legacy v1 admission as global-only, and continue exercising hundreds of concurrent pool-global reservations.

### 2026-08-08: authoritative private epoch state

- Catalog schema v12 stores each private epoch under an independent authoritative SlateDB key, binding the pool UUID, reservation UUID, exact branch UUID, epoch, database identity, revision, state, and lifecycle timestamps. PostgreSQL and JSON projections remain identical and do not receive epoch state.
- Monotonic lifecycle: exact registration begins at revision one in `open`; revision-fenced transitions permit only `open` to `sealed_private` and `open`/`sealed_private` to permanent `exposed`. Exact retries are generation-neutral, conflicting reservation identities fail, and no transition reopens an epoch. Branch deletion atomically exposes every remaining private epoch before its root can leave the live branch record.
- Conservative enablement: the record is necessary metadata but never deletion authority by itself. The normal writer remains ownerless, storage-marker verification is not yet wired to registration, sealing does not yet drain publishers, and no non-expiring local-GC batch guard exists; shared-pool local reclamation therefore stays disabled.
- Executable proof: focused tests cover exact registration replay, conflicting identities, branch-bound sealing, permanent exposure, rejected reopen, deletion-time exposure, snapshot validation, and migration of every supported v2-v11 catalog without rewriting existing records.

### 2026-08-08: storage-authenticated private epoch registration

- The private-epoch lifecycle now rereads the exact permanent reservation from the segment pool and accepts only authenticated v2 markers with a non-nil branch owner. Pool UUID and reservation UUID are never supplied by the caller; the catalog copies them, along with the epoch, branch UUID, and database identity, from HMAC-verified storage evidence.
- Registration compares the requested exact branch incarnation and database identity with the authenticated marker, then requires that identity to equal the ready branch's authoritative durable-root identity before new SlateDB publication. Stable operation timestamps make ambiguous retries reconcile by immutable registration identity even after a concurrent monotonic transition to `exposed`; they never reopen the epoch. A different owner, reservation, database, original timestamp, or branch root fails closed.
- Legacy v1 and ownerless v2 reservations remain valid only for pool-global uniqueness and cannot enter the private catalog lifecycle. PostgreSQL and JSON remain identical root-free projections and receive none of this storage authority.
- Conservative enablement remains in force: collectors must reauthenticate the permanent marker, prove a sealed publisher barrier, and hold a durable non-expiring exclusion guard before local deletion. This slice enables none of the physical reclamation path.
- Executable proof covers exact authenticated registration/retry, retry after deletion has permanently exposed the epoch, rejected owner and database substitutions, ownerless-marker rejection, and reservation-HMAC tampering.

### 2026-08-08: durable local-GC exclusion guards

- Catalog schema v13 stores a durable independent guard for one exact branch UUID, `sealed_private` epoch revision, bounded candidate count, and candidate-set digest. Acquisition and epoch-state validation share the catalog's atomic SlateDB mutation lock; exact retry is generation-neutral and a second guard for the epoch conflicts.
- Conservative root gate: acquisition fails while the branch has any checkpoint, while any catalog lease exists, while an incomplete clone-source operation derives from the branch, or while any global GC run still retains captured roots. This deliberately favors retention where a deleted-checkpoint lease cannot be mapped back to its branch without weakening the lease model.
- Exact clone-source binding: reservation requires the incomplete operation's immutable `parent_id` to equal the source checkpoint's authoritative branch UUID. Snapshot validation rechecks the live checkpoint binding and, after deletion, requires the checkpoint tombstone's preserved former branch UUID to match; missing or mismatched migrated history fails closed. This makes a RootCreated operation remain attributable to—and block a guard on—its true source branch; ready-branch ancestry remains historical metadata.
- Non-expiring exclusion: a live guard blocks permanent epoch exposure, branch deletion, checkpoint publication, clone-source capture, and new branch/checkpoint leases. There is intentionally no timeout, revocation, or generic release mutation; only a fully classified bounded deletion-progress transition may retire it.
- Storage split and enablement: guards remain SlateDB-only authority and never enter identical PostgreSQL/JSON customer projections. Exact local liveness enumeration and the physical deletion worker are not yet implemented, so private physical deletion remains disabled.
- Executable proof covers lease-blocked acquisition, exact acquisition retry, exposure/deletion/checkpoint/lease fencing, rejection of a misbound clone parent, blocking by an incomplete RootCreated clone after source-checkpoint deletion, durable close/reopen recovery, sealed-state preservation, and migration of every supported v2-v12 catalog without rewriting existing records.

### 2026-08-08: publisher-drained private epoch sealing

- The extent store now rotates writer epochs through an atomic in-memory segment-store handle. Rotation is serialized under the exclusive FrameLoc publication barrier, database flush barrier, and append gate; it drains background seals, durably PUTs the old open segment, flushes all committed old-epoch references, and installs a fresh counter-zero namespace before writers resume.
- A move-only publisher-drain receipt binds the process-unique identity of the exact live `ExtentStore` plus the old and successor epochs. The sealing lifecycle is explicitly attached to that writer instance and rejects a genuine receipt from a separately constructed/dummy store even for the same epoch pair. It then reauthenticates both permanent v2 branch reservations and uses one atomic catalog transition to revision-fence both authoritative records, require the same branch UUID, database root, and pool plus an `open` successor, and change only the old `open` epoch to `sealed_private`. Exact retry accepts the same seal after a later monotonic exposure without reopening it.
- Concurrent rotations reload the active writer only after acquiring the barriers, so they form a strict chain rather than issuing two receipts from one old epoch. Invalid zero/self rotations fail before publication resumes.
- Crash behavior is retention-safe: losing the in-memory receipt before catalog sealing leaves the old epoch `open` and permanently ineligible for local GC; global GC remains available. The normal standalone server still reserves ownerless epochs, and no private deletion path is enabled by this slice.
- Executable proof writes through the old epoch, rotates and verifies zero unflushed bytes, writes through the successor while preserving old readability, races two blocked rotations and verifies a strict chain, rejects a genuine same-pair receipt minted by a different dummy store, proves a stale successor revision atomically rejects sealing without changing the old epoch, then integrates the bound live-writer receipt with two storage-authenticated catalog records, exact sealing retry, and retry after monotonic exposure.

### 2026-08-08: durable bounded local-GC progress

- Catalog schema v14 stores one independent progress/audit record keyed by the durable local-GC guard. The record repeats the immutable branch, sealed epoch revision, bounded candidate count, and candidate-set digest, while retaining only a cursor and aggregate deleted-object, deleted-byte, and already-absent outcomes.
- Progress is revision-fenced and monotonic. The cursor must exactly equal deleted plus already absent and cannot cross the guard's fixed candidate count; identity, start time, and aggregate counters cannot move backwards. Exact retries are generation-neutral.
- The fully classified final publication writes a completed audit and retires the non-expiring guard in the same SlateDB batch. Partial progress cannot retire the guard; a lost completion response reconciles against the durable audit after the guard is gone. PostgreSQL and JSON remain identical and receive none of this storage authority.
- Conservative enablement remains in force: this slice does not enumerate candidates, validate their exact immutable object identities, hold the filesystem publication barrier through deletion, or issue physical deletes. The shared-pool local collector therefore remains disabled.
- Executable proof covers durable guard recovery, exact progress replay, malformed aggregate rejection, monotonic advance, atomic completion/guard retirement, lost completion-response replay, post-completion exposure, and migration of every supported v2-v13 catalog without rewriting existing records.

### 2026-08-08: exact sealed-epoch candidate preparation

- The extent store prepares candidates only for a nonzero epoch different from its active writer; globally unique reservations are not numerically ordered, so the later authenticated catalog attachment—not integer comparison—proves the term was rotated away and sealed. Preparation holds the exclusive FrameLoc-publication and database-flush barriers, seals the active successor before the database-wide metadata flush, scans the durable exact-epoch counter range, sorts by segment identity, and applies the same fixed 256-candidate bound as the catalog guard.
- A zero-live counter is only a hint. Each object must pass its authenticated reverse-directory check against both current and durable forward maps; a corrupt under-count that still has a live `FrameLoc` is retained. Transient storage failures abort preparation and malformed or ambiguous local metadata never becomes a candidate.
- Candidate identity includes the immutable segment ID, appended-byte counter, object size, modification timestamp, and a full streamed SHA-256 content digest. A domain-separated digest binds the ordered bounded set and the exact live `ExtentStore` publisher identity accompanies the prepared batch.
- Conservative enablement remains in force: the prepared batch is not yet persisted or atomically attached to guard acquisition, and there is no barrier-through-delete worker. This primitive issues no deletes and shared-pool local reclamation remains disabled.
- Executable proof creates a genuinely dead old-epoch segment, corrupts a still-live segment's counter to zero, rotates away from the epoch, commits a RAM-only successor write, proves preparation seals that successor before its global flush and that the durable pointer resolves directly through the object store, proves deterministic retry and exact digest stability, selects only the directory-verified dead object, and rejects current-epoch and invalid bounds.

### 2026-08-08: immutable private-candidate artifact and guard attachment

- The exact bounded descriptor set is canonically encoded under `private-gc-artifacts/<guard UUID>.bin`, binding a format magic, guard UUID, exact live `ExtentStore` publisher identity, its permanently bound branch UUID and database identity, sealed epoch, count, candidate digest, and every segment/object identity. Conditional create reconciles an ambiguous exact retry byte-for-byte and rejects reuse of the UUID for changed candidates.
- Successful immutable publication returns an opaque move-only capability. The bound private-epoch lifecycle accepts only a capability whose publisher, branch UUID, and database identity match its exact live writer and the guard request; it then reauthenticates the permanent branch-owned epoch marker, requires the matching authoritative branch/database and exact `sealed_private` revision, and derives the guard's count/digest rather than trusting caller metadata. The atomic guard mutation still rechecks ready-branch state and every root blocker.
- Artifact keys and guard state remain internal object-store/SlateDB authority and never enter the identical PostgreSQL/JSON customer projections. An artifact published before a failed guard acquisition is harmless retained metadata and can be cleaned only by a later bounded metadata-retention policy.
- Conservative enablement remains in force: restart-time artifact decoding/revalidation and the barrier-through-delete progress worker are not implemented, so this slice exposes no production physical-delete path.
- Executable proof covers exact artifact retry, rejection of one operation UUID after candidate storage changes, live-publisher guard attachment, storage-marker and sealed-revision binding, exact guard retry, the existing root/exposure fences before completion, and a two-branch same-pool attack in which a writer bound to branch A prepares branch B epoch bytes but cannot attach them to branch B's valid sealed guard.
