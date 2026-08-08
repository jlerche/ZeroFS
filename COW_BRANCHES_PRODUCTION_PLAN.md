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

- [ ] Implement logical checkpoint deletion.
  - [ ] Fence only branch creations that have not yet established independent durable roots.
  - [ ] Do not block deletion because a ready branch records the checkpoint as historical provenance.
  - [ ] Preserve ready descendant branches.
- [ ] Add race tests.
  - [ ] Delete while branch creation has not started cloning.
  - [ ] Delete while clone storage exists but the branch is not yet published.
  - [ ] Delete after the branch is ready.
  - [ ] Retry after an ambiguous deletion response.

### Epic 3.3: Delete branches with descendants

- [ ] Implement logical branch deletion without recursively deleting descendants.
  - [ ] Tombstone the exact branch UUID.
  - [ ] Preserve descendants' roots, mountability, and write behavior.
  - [ ] Preserve enough historical metadata to explain lineage.
  - [ ] Prevent stale requests from deleting a new branch that reused the old name.
- [ ] Add descendant tests.
  - [ ] Delete a parent with one child.
  - [ ] Delete a middle branch in a deep lineage.
  - [ ] Delete ancestors in different orders.
  - [ ] Delete a branch while descendants are mounted or being created.
  - [ ] Recreate a deleted branch name and verify identity isolation.

## Phase 4: Implement production garbage collection

### Epic 4.1: Build a root-capture protocol

- [ ] Begin each global GC run by reading catalog generation `G`.
- [ ] Capture exact immutable root identities for all live branches, checkpoints, and leases.
- [ ] Pin the captured roots for the duration of the run.
- [ ] Record a physical-inventory cutoff so newer objects are never eligible in that run.
- [ ] Persist a compact resumable run record.
  - [ ] Run UUID.
  - [ ] Catalog generation.
  - [ ] Inventory cutoff.
  - [ ] Captured root identities or a digest plus immutable root-list object.
  - [ ] Mark-shard locations.
  - [ ] Current phase.
  - [ ] Quarantine timestamp.
- [ ] Abort safely when a root cannot be opened, authenticated, or enumerated.

### Epic 4.2: Stream the reachable set

- [ ] Enumerate segment references from each pinned root exactly once per run.
- [ ] Avoid `candidate count × branch count × checkpoint count` point-read behavior.
- [ ] Emit reachable segment IDs into bounded sorted runs.
  - [ ] Partition runs by a stable segment-ID prefix.
  - [ ] Bound memory independently of total live storage.
  - [ ] Persist checksums and run metadata.
- [ ] Merge and deduplicate sorted runs into authoritative mark shards.
  - [ ] Make every shard independently verifiable.
  - [ ] Treat missing or corrupt shards as a failed run.
  - [ ] Permit Bloom filters only as performance hints, never as deletion authority.
- [ ] Measure work proportional to reachable references rather than repeated remote view probes.

### Epic 4.3: Inventory and quarantine unreachable objects

- [ ] Stream the physical segment inventory by the same stable shard key.
- [ ] Join each inventory shard against its authoritative mark shard.
- [ ] Exclude objects newer than the inventory cutoff.
- [ ] Write unreachable candidates to a durable quarantine set.
- [ ] Do not physically delete during the first unreachable observation.
- [ ] Record reasons that prevent deletion, including missing roots, corrupt metadata, generation changes, and lease uncertainty.

### Epic 4.4: Revalidate and delete

- [ ] Re-read the catalog after marking and require generation `G` before accepting the run.
- [ ] Wait a configurable grace period that exceeds relevant lease, propagation, and clock-skew bounds.
- [ ] Perform a second independent reachability observation or equivalent generation-fenced validation.
- [ ] Remove candidates that became reachable or cannot be proven unreachable.
- [ ] Delete remaining objects in bounded idempotent batches.
- [ ] Persist batch progress so crashes resume safely.
- [ ] Retain deletion audit metrics without retaining unbounded per-object metadata.

### Epic 4.5: Add fast local GC

- [ ] Define a proof that a segment is private to one exact branch incarnation.
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
- Independent review gate: review of `31efd05` found cross-kind/tombstone UUID reuse, global-generation contention, projection parity, deleted-lineage reconstruction, identifier-bound, and schema-upgrade issues. The follow-up correction globally reserves UUIDs, uses per-record revisions, retains root-free historical lineage in tombstones, aligns JSON/PostgreSQL behavior, adds bounded crash-resumable SlateDB migrations through the current v4 lease schema, and adds adversarial tests.

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

- Catalog lifecycle: schema v3 adds independently keyed permanent create-operation records with only `reserved`, `root_created`, and `published` phases. Reserved operations root the exact source; root-created operations root source plus destination; published records enumerate no GC roots and remain only as operation-UUID/idempotency reservations.
- Atomic boundaries: reservation creates the `Creating` branch, name index, operation, and exact source-hold index in one durable batch. Root recording adds the authenticated destination as an incomplete root. Publication re-authenticates storage, atomically changes the branch to `Ready`, retains the exact destination head, marks the operation published, and removes the source hold. Raw catalog mutations are crate-private so external callers cannot bypass the storage-verifying lifecycle coordinator; its factory rejects catalog/branch namespace overlap before storage I/O.
- Retry and race proof: the safe name-based entry point resolves once to the catalog checkpoint UUID and encoded SlateDB checkpoint/manifest identity. Exact retries return the existing generation/result, changed immutable inputs or roots conflict, checkpoint deletion and reservation serialize under the exact source hold, snapshots fail closed if that derived hold index diverges from incomplete operations, completed retries do not require a deleted historical source, and generic mutations cannot create or rewrite `Creating` lifecycle state.
- Projection boundary: create-operation phases, source holds, and both durable roots exist only in authoritative SlateDB. JSON and PostgreSQL continue to receive the same root-free branch/checkpoint/tombstone projection.

### 2026-08-08: bounded authoritative leases

- Lease authority: catalog schema v4 stores leases and permanent lease-UUID tombstones only in SlateDB. Each lease binds an exact subject kind/UUID/root, read/write mode, revision, issuance/renewal/expiry times, and SHA-256 renewal-token binding; PostgreSQL and JSON remain unchanged and root-free.
- Acquisition and renewal: the public coordinator authenticates storage before an atomic exact-resource revision/root/state check and lease insertion. Name lookup is only the first locator and the request carries the stable subject UUID. Renewal requires the exact UUID, token, expected revision, unexpired lease, unchanged root, and still-mountable subject. The expected revision plus requested duration is the scoped idempotency key: exact retries reconcile an applied batch with a lost response, while timestamps and expiry can only move forward.
- Expiry and GC: leases are bounded to five minutes. Release is exact and idempotent; crash cleanup expires only after the lease deadline plus 30 seconds of conservative clock skew. Live lease roots participate directly in generation-tagged root snapshots, survive logical subject deletion, and cannot be renewed after deletion or resurrected after expiry/tombstoning.
