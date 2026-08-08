# Copy-on-write branch semantics and safety invariants

Status: normative implementation contract for `jacob/cow-branches-production`.

This document defines the smallest storage and lifecycle model that production
copy-on-write branches must implement. “Must” and “must not” are safety
requirements. An implementation that needs additional coordination metadata
must identify the invariant that requires it before adding that metadata.

## Scope and authority

- The dedicated SlateDB catalog is authoritative for branch/checkpoint identity,
  lifecycle state, durable roots, leases, incomplete operations, generations,
  and tombstones.
- PostgreSQL and JSON contain the same reconstructible customer-facing
  projection. They never contain durable roots or manifests and never decide
  mountability, mutation eligibility, or garbage collection.
- Immutable storage objects and immutable root manifests are the data plane.
  Catalog ancestry is history, not a chain that must remain live to read data.
- Branch work must not redesign 9P, FUSE, TLS, authentication, CSI, NBD,
  replication, or unrelated dependency policy. A cross-subsystem change must
  cite a stable invariant ID from this document and land separately.

## Stable invariant index

- **ID-1 — incarnation isolation:** resource UUIDs are permanent identities and
  are never reused; names never authorize destructive work.
- **ROOT-1 — independent readiness:** a `Ready` branch owns a durable immutable
  root that remains complete without any ancestor or source checkpoint.
- **PUB-1 — durability before visibility:** a root becomes visible only after
  its storage state is durable, and root/index publication is atomic.
- **DEL-1 — namespace first:** deletion rejects new name opens and commits at
  its linearization point, while physical reclamation is asynchronous.
- **LEASE-1 — acquire/delete serialization:** exact lease insertion and
  resource-state/root validation are one atomic per-resource decision made
  before the data plane is exposed.
- **CAT-1 — validated atomic roots:** catalog root additions/removals, indexes,
  holds, and generations change in validated durable atomic batches.
- **GC-1 — uncertainty retains:** missing, corrupt, unauthenticated, ambiguous,
  or changing evidence can retain data but can never authorize deletion.
- **GC-2 — two fenced observations:** physical deletion requires two complete
  generation-fenced unreachable observations separated by the grace period.

## Identities and names

- A volume, branch, checkpoint, lease, lifecycle operation, and GC run has a
  stable, non-nil UUID.
- Resource UUIDs are never reused, including after logical deletion. Tombstone
  cleanup must leave a compact permanent UUID reservation or an equivalent
  non-reuse proof.
- A name is a mutable lookup key, not an identity. Destructive and retryable
  requests carry the exact UUID (and operation UUID where applicable).
- A deleted name may be reused only by a new UUID. A stale request for the old
  UUID must return the old result or an identity conflict; it must never act on
  the new resource.
- User names must pass the catalog bounds and must not begin with the exact
  ASCII byte prefix `__zerofs_`. Prefix matching is bytewise and case-sensitive;
  this reserves the existing `__zerofs_branch_create_` and
  `__zerofs_branch_publication_` families plus future internal checkpoints.

## Durable-root model

Every `Ready` branch owns an independent immutable durable-root identity. That
root must be sufficient to open every immutable object the branch may read
without opening its parent branch or origin checkpoint. `parent_id` and
`origin_checkpoint_id` may reference live or tombstoned UUIDs and may eventually
become dangling display history after tombstone compaction; neither field is a
storage-liveness edge.

The storage adapter must prove the following before branch lifecycle code is
enabled:

1. Creating or cloning a SlateDB checkpoint produces an exact immutable source
   identity.
2. Creating the destination root durably pins all data reachable from that
   source identity.
3. Removing the source checkpoint cannot invalidate the destination root.
4. Opening a root either authenticates and enumerates its complete reachable
   state or fails closed.

If SlateDB’s native clone/final-checkpoint mechanism does not establish all four
properties, the implementation adds the smallest pin manifest that does. A
catalog record alone is not proof that storage is durable.

For the pinned SlateDB revision, the production adapter therefore treats a root
as the destination database path plus an operation-scoped internal checkpoint
UUID and its exact manifest number. That destination checkpoint pins the branch
manifest. The adapter derives the destination path as the configured private
branch-database prefix plus the destination UUID; callers cannot select an
arbitrary object-store namespace. The fully derived path is bounded and must be
disjoint from the source namespace: equality and either ancestor/descendant
relationship are rejected before storage I/O. A publishable source and
destination root must be fully flushed: an exact manifest with an outstanding
WAL range is rejected rather than treating a separately configured WAL store as
an implicit root dependency. Each external
database entry in the destination manifest names physical source SSTs and an
unnamed, non-expiring final checkpoint that pins them. Root authentication
requires the exact canonical operation result, checks every pin's checkpoint
manifest covers the SST IDs attributed to that physical source, enumerates the
unsegmented and segmented manifest trees, and confirms every resolved SST object
exists with bounded concurrent metadata requests. Missing or unreadable
evidence fails closed. SlateDB validates SST
checksums when content is read; any later corruption fails the open/read and
retains data rather than authorizing GC. The customer-named source checkpoint
and catalog ancestry may be deleted after publication; physical source
namespaces and final pins remain immutable storage dependencies until SlateDB
compaction and detach remove every external SST reference.

Before clone I/O, the adapter conditionally creates an immutable owner descriptor
inside the destination namespace. It binds the operation UUID, destination UUID
and path, and exact source path/checkpoint/manifest, preventing retries or
concurrent callers from adopting storage created for different inputs. An
immutable result descriptor elects one exact destination checkpoint when
identical calls race. If every racer dies before election, the retry
deterministically chooses the first `(manifest number, checkpoint UUID)` among
the exact operation-name checkpoints; the immutable result creation remains the
linearization point. Losing checkpoints are never valid roots and are removed
best-effort on every retry. Applied writes with lost responses are reconciled by
exact descriptor contents and authenticated storage state.

These two descriptors are the single invariant-required storage proof for one
clone, not customer projections or lifecycle authority. Their locations are
derived from the destination root. The authoritative catalog operation in
`reserved` phase owns the owner descriptor and conservatively roots the source
plus destination namespace; in `root_created` phase it stores the exact
`DurableRoot`, which must equal the immutable result descriptor. A live branch or
lease root likewise owns that derived proof. Only a catalog batch may make the
branch `Ready`, and a descriptor without the matching catalog operation/live
root cannot do so. Additional sequential step receipts remain prohibited.

Noncanonical operation checkpoints are best-effort cleanup targets and are
retained on uncertainty. The owner descriptor, canonical result descriptor,
canonical checkpoint, and branch database namespace remain root metadata until
the catalog has tombstoned or aborted the exact destination, all leases and
incomplete-operation holds are gone, and generation-fenced GC has established
the namespace is unreachable. Cleanup is idempotent; failures leak storage and
are retried by exact-operation reconciliation or later GC.

## Branch state machine

`Deleted` is represented by removal of the live record plus a tombstone. The
allowed state transitions are:

| From | To | Externally visible | Preconditions and linearization |
| --- | --- | --- | --- |
| absent | `Creating` | inspect by exact UUID; not list/open by ordinary name | Atomically reserve branch UUID, name, operation UUID, and immutable source identity. |
| `Creating` | `Ready` | yes | Destination durable root exists and is readable; one catalog batch publishes that exact root and clears the incomplete operation. |
| `Creating` | tombstone | no new opens | Reconciliation proves no published destination root, or cleanup safely removes/retains any orphan root before tombstoning. |
| `Ready` | `Deleting` | no new opens by name after this transition | Exact UUID/revision matches; atomically remove the name index and publish deletion intent. |
| `Deleting` | tombstone | visible as deleted history | Live GC root is removed atomically with a root-free tombstone; leases keep their exact roots independently live. |

No other transition is valid. `Ready` is monotonic: a branch never returns to
`Creating`. A tombstoned UUID never becomes live again.

### Create and retry contract

A create request contains `operation_id`, destination UUID/name, exact source
checkpoint UUID and immutable manifest identity, and (for history) parent UUID.

- The same operation ID with identical immutable inputs returns the existing
  in-progress or completed result.
- The same operation ID with different inputs returns an operation conflict.
- A different operation targeting a reserved UUID or live name returns an
  identity/name conflict.
- The source checkpoint remains held as a GC root throughout `Creating`.
  Checkpoint deletion and this hold are serialized in one catalog mutation
  domain for that exact checkpoint/operation. After storage establishes the
  destination root, the operation records that root as an additional incomplete
  GC root. The `Ready` publication batch atomically converts it to the live
  branch head and removes the source hold; there is no interval with neither.
- Publication is the transition to `Ready`, after destination-root durability.
  A response lost after publication is recovered by reading the operation and
  branch records; publication is not repeated with a different root.
- A process/object-store failure before a known durable destination root leaves
  `Creating`. Recovery retries the same idempotent storage operation or proves
  the orphan safe to remove. Ambiguity retains data.
- Creating from a live head first flushes it, creates an internal immutable
  checkpoint, and invokes this exact checkpoint-based primitive. The internal
  named dependency is removed after independent destination publication.

For `Creating` to tombstone, the same create/cancel operation observes the
recorded phase and idempotently completes cleanup or retention before publishing
the tombstone. Retrying that exact operation returns the same aborted outcome;
a different operation ID conflicts. An authorized repair request for the exact
UUID may resume the recorded operation, but no unbounded global recovery sweep
is required.

The authoritative catalog recovery record is deliberately small: operation
UUID, destination UUID and name, exact source UUID/manifest, optional parent
UUID, and phase (`reserved` or `root_created` with the resulting destination
root). The derived immutable storage owner/result proof described above is the
minimum conditional-write evidence required by ROOT-1 and PUB-1; it does not add
catalog phases. Additional per-step receipts, takeover generations, activity
fences, and recovery sweeps are prohibited until a demonstrated failure case
cannot be reconciled from these fields.

The implementation retains the operation UUID after publication as a compact
`published` idempotency reservation containing the same immutable request and
initial result. It is not an incomplete operation and enumerates no GC roots;
the live branch head and leases are authoritative after publication. This
permanent reservation prevents rebinding an old operation UUID after name reuse
and lets a lost publication response return success even after the historical
source checkpoint is deleted or the branch head later advances.

## Checkpoint state and deletion

A named checkpoint is a stable UUID plus exact immutable root. Creation becomes
visible only after that root is durable. A checkpoint may be deleted even when a
`Ready` branch records it as historical origin.

Checkpoint deletion atomically removes the name and named GC root for the exact
checkpoint UUID and writes its tombstone. It conflicts only with an incomplete
branch creation holding that checkpoint as its source. It does not conflict with
a ready descendant, because the descendant has its own root.

Checkpoint mounts are read-only leases bound to the exact checkpoint UUID and
root. Lease acquisition and the checkpoint deletion fence serialize in the same
exact checkpoint domain: a lease that wins remains an independent GC root, while
deletion prevents any later mount and may remove the named root immediately.

Repeated deletion of the same checkpoint UUID returns the existing tombstone as
success. Deletion by a reused name without the expected UUID is invalid. A lost
success response is resolved by reading the exact UUID/tombstone.

## Branch deletion, descendants, and mounts

Deleting a branch never recursively mutates descendants. At the deletion
linearization point the exact branch UUID/revision moves to `Deleting`, its name
stops resolving for new opens, and its durable root remains protected until the
final catalog/tombstone mutation and any lease requirements are satisfied.

- Descendant roots, names, writeability, and ancestry metadata are unchanged.
- Commits and deletion serialize in the same exact branch/revision domain. A
  commit that wins first advances the branch root before deletion; after the
  deletion transition, every new or pending commit is rejected.
- Existing mounts continue read-only under their exact bounded leases. Deletion
  does not extend a lease and renewal after deletion is rejected. The data plane
  must observe the deletion fence before acknowledging any later write.
- The `Deleting` record retains the final branch root until every writer lease
  is released or conservatively expired. It may then atomically remove that GC
  root and publish the root-free tombstone. Reader leases independently retain
  the exact immutable roots from which those readers continue.
- A client without a lease loses access at deletion. A client with an unexpired
  lease may read its exact root until lease expiry or explicit release.
- A delete request carries a deletion operation UUID. `Ready` to `Deleting`
  records it. An exact retry that observes `Deleting` resumes the lease-drain and
  finalization decision; a different operation conflicts. An exact retry that
  observes the tombstone returns success. Thus a crash or lost response on
  either side of both deletion transitions is recoverable from authoritative
  state. Reconciliation is request-driven or targets an explicitly inspected
  UUID; it is not an unbounded recovery sweep.
- Repeated deletion of the same UUID is idempotent and returns the current
  `Deleting` result or the completed tombstone without restarting deletion.
- A deletion carrying the old name/UUID cannot delete a new incarnation that
  reused the name.
- Physical object deletion is always asynchronous and is performed only by GC.

## Lease contract

A lease records a lease UUID, subject kind (`branch` or `checkpoint`), exact
subject UUID and root identity, access mode (`read` or `write`), issued and expiry
times, monotonically increasing revision, and a random renewal token or
equivalent unforgeable binding. It is an authoritative GC root. Checkpoint
leases are always read-only; write leases are valid only for branches.

The production catalog caps every acquisition or renewal interval at five
minutes and retains an expired root for an additional 30-second clock-skew
window before an expiry mutation may tombstone it. Only a SHA-256 binding of
the caller-held UUID renewal token is stored. A compact permanent lease
tombstone retains that binding and UUID so exact release/expiry retries are
idempotent and the lease incarnation can never be recreated.

- Acquisition first authenticates that the candidate root is readable. It then
  atomically revalidates the exact subject kind/UUID, revision, mountable state,
  and unchanged root identity while inserting the authoritative lease in the
  same resource mutation domain. No data-plane handle or byte is exposed before
  that batch is durable. Exact resource deletion serializes with this batch, so
  exactly one of lease acquisition or the deletion fence wins.
- Renewal must match lease UUID, subject kind/UUID, root, access mode, token, and
  revision, and must occur before expiry. The expected revision and requested
  duration form a lease-scoped idempotency key, so an applied batch with a lost
  response is reconciled without extending the interval again. It produces a
  higher revision and bounded, nondecreasing expiry; renewal time cannot move
  backward. Once deletion has fenced either a branch or checkpoint, renewal is
  rejected so every deleted subject has a bounded drain time.
- Release is idempotent for the exact lease UUID/token.
- Shutdown attempts release but correctness relies on expiry after a crash.
- Once expired or released, a lease UUID/token pair can never be resurrected.
- Clock uncertainty extends retention; it never shortens it.

A writer lease authorizes commits only while the branch remains `Ready`.
Committed writes advance the authoritative branch head before acknowledgement;
the lease itself never owns an unpublished mutable head. `Deleting` turns all
remaining writer leases into read-only retention roots and waits for their
release/expiry before removing the final branch head.

## Catalog consistency

- Each successful mutation durably changes a monotonically increasing snapshot
  generation. Per-record revisions fence conflicting updates without making
  unrelated resources share one optimistic-CAS failure domain.
- A mutation that publishes or removes a GC root and its associated state/name
  indexes is one durable atomic batch.
- Callers retry only exact idempotent operations and use a bounded policy. The
  catalog returns revision, identity, or operation conflicts; it never retries
  forever internally.
- A snapshot is either internally validated at one generation or rejected. Bad
  indexes, duplicate identities/names, invalid records, unreadable data, or an
  unsupported schema fail closed.

## Garbage-collection invariants

### Authoritative roots

For a captured catalog generation, the complete root set is:

1. every live `Ready` branch head;
2. every `Deleting` branch's retained final head until tombstone publication;
3. every live named checkpoint;
4. every destination root recorded by an incomplete create;
5. every exact source root held by an incomplete create;
6. every unexpired branch or checkpoint lease root, including conservative
   clock skew;
7. replication or recovery roots that can expose old data;
8. immutable roots pinned by an accepted in-progress GC run.

Ancestry UUIDs, PostgreSQL rows, JSON rows, names, counters, Bloom filters, and
unverified cache entries are not GC roots or evidence of unreachability.

### Eligibility for physical deletion

An object may be deleted only when every condition below is proven:

- it is absent from the complete reachable set of an authenticated root capture;
- its physical creation/version predates that run’s inventory cutoff;
- all mark shards needed for its partition exist and pass checksums;
- the catalog generation and captured root-list identity remain valid at mark
  acceptance;
- it remains absent in a second independent, generation-fenced observation after
  a grace period exceeding lease duration, propagation delay, and clock skew;
- it is not protected by an active lease, recovery record, replication root, or
  pinned GC run; and
- no relevant metadata was missing, corrupt, unreadable, unauthenticated, or
  ambiguous.

Failure to prove any condition retains the object.

### Streaming mark, inventory, quarantine, and delete

1. Capture generation `G`, immutable root list/digest, and an inventory cutoff;
   pin the list for the run.
2. Enumerate each root once and emit segment IDs into memory-bounded sorted runs
   partitioned by a stable segment-ID prefix.
3. Merge/deduplicate runs into checksummed authoritative mark shards. Bloom
   filters may avoid work but never authorize deletion.
4. Stream physical inventory by the same partitions, exclude objects newer than
   the cutoff, and join with complete mark shards.
5. Persist first-observation candidates in quarantine; do not delete them.
6. Re-read the catalog, require `G`, and accept or abort the mark. Catalog change
   aborts safely rather than attempting speculative reconciliation.
7. After the grace period, perform a second complete observation with a new
   generation fence. Remove any reachable or uncertain candidate.
8. Delete proven candidates in bounded idempotent batches and durably checkpoint
   batch progress.

The run record contains only run UUID, generation, cutoff, immutable root-list
identity/digest, mark-shard locations/checksums, phase, and quarantine time.
Missing state prevents deletion. Interrupted work resumes exactly or aborts and
leaks storage. Delete retries are idempotent.

### Private fast path and cleanup

Local GC may bypass global marking only when an authenticated ownership record
proves a segment was created by one exact branch incarnation and was never
published into a checkpoint, clone, lease for another root, replication root, or
shared manifest. Ambiguous ownership falls back to global retention.

Tombstones may be compacted only after no lease, recovery operation, projection
reconciler, or retained GC generation can observe them. UUID non-reuse must
survive compaction. Completed run records, marks, and quarantine artifacts are
removed idempotently in bounded passes after retention windows.

## Research-branch inventory

The research branch is test and product-specification input, not a merge source.
The production implementation may port individual cases only after mapping them
to an invariant above.

| Research artifact | Reusable behavior | Production disposition |
| --- | --- | --- |
| `branch_registry.rs` identity/name/limit tests | Stable UUIDs, name isolation, bounds, exact retry conflicts | Port focused cases to the independent-key catalog; reject the global JSON CAS document. |
| `branch_registry.rs` checkpoint/create race tests | Either delete wins before a source hold or the exact create hold wins | Re-express as one checkpoint/operation catalog transaction; reject broad activity fences. |
| `branch_manager.rs` ambiguous create and publication tests | Recover lost responses from durable identity and root state | Port scenarios to the two-phase minimal recovery record; reject receipts/takeover protocols. |
| `branch_mount.rs` mount/delete race tests | Mount and delete resolve by exact branch/lease UUID, never name alone | Port to bounded leases; reject unbounded persistent mount intents. |
| `branch_gc.rs` corruption and retry tests | Missing roots/manifests, corrupt metadata, or retry exhaustion retain data | Port as fail-closed model/fault tests; replace candidate-by-view probing. |
| `branch_gc.rs` scale/work-bound tests | Work and memory need explicit ceilings | Port assertions to streaming root/reference/inventory accounting. |
| `checkpoint_manager.rs`, `cli/checkpoint.rs`, and checkpoint documentation | Exact checkpoint UUID/name create, list, inspect, mount, and delete; public/internal name separation | Preserve the public behavior and exact-delete races; replace research lifecycle fencing with the catalog source hold. |
| branch CLI/RPC/proto and branch documentation | Branch `create`, `list`, `inspect`, `mount`, `delete`; UUID, state, parent, origin | Treat names and response fields as an API draft, then implement narrowly. |
| stable catalog generation, origin UUID, segment ownership ideas | Snapshot fencing, historical audit, conservative private reclamation | Preserve in the smaller catalog/root/GC model. |

### Named research tests selected for porting

These are behavioral inputs, not code to copy. Tests tied only to rejected
receipts, takeovers, expiry buckets, or the global registry are intentionally not
ported unless their underlying scenario appears here.

| Research test | Required production assertion |
| --- | --- |
| `reserve_is_idempotent_and_ready_is_monotonic` | Exact create retries converge and `Ready` never regresses (`ID-1`, `PUB-1`). |
| `checkpoint_delete_fence_wins_before_branch_reservation` | Delete prevents a later source hold for that checkpoint (`CAT-1`). |
| `creating_branch_reservation_wins_before_checkpoint_delete` | An established exact source hold prevents checkpoint-root removal (`CAT-1`). |
| `exact_creating_retry_cannot_bypass_a_preexisting_delete_fence` | Retry identity does not weaken the winning delete decision (`ID-1`). |
| `initialization_reconciles_a_lost_create_response` | Ambiguous durable publication is recovered by exact identity (`PUB-1`). |
| `forced_delete_can_abort_creating_branch_but_is_anchored_to_its_id` | Cancellation cannot affect a name-reuse incarnation (`ID-1`). |
| `ambiguous_registry_cas_reconciles_the_same_mount_uuid` | Ambiguous lease insertion resolves by exact lease UUID (`LEASE-1`). |
| `exact_checkpoint_mount_and_delete_are_one_cas_gate` | Checkpoint lease acquisition and exact deletion have one winner (`LEASE-1`, `CAT-1`). |
| `branch_mount_and_delete_race_has_exactly_one_winner` | No data plane is exposed without a lease that beat deletion (`LEASE-1`). |
| `unsafe_exit_retains_and_quiescence_releases` | Crash retention is conservative and clean release is idempotent (`GC-1`). |
| `create_operation_id_is_exactly_idempotent_and_single_use` | An operation UUID cannot be rebound to different inputs (`ID-1`). |
| `deleted_create_operation_cannot_bind_to_name_reuse` | Old operation state cannot bind a new name incarnation (`ID-1`). |
| `publication_marker_pins_incarnation_through_manifest_gc_until_hold_cleanup` | Source and destination root handoff never leaves a GC gap (`PUB-1`, `CAT-1`). |
| `source_checkpoint_delete_waits_until_clone_owns_its_final_pin` | Source deletion waits until the destination is independently rooted (`ROOT-1`). |
| `exact_checkpoint_clones_are_independent_and_keep_immutable_provenance` | Ready descendants survive source deletion and retain history (`ROOT-1`). |
| `deletion_waits_for_mount_and_cleans_pins_metadata_and_owned_segments` | Namespace deletion fences writes; root removal waits for lease safety (`DEL-1`, `LEASE-1`). |
| `publication_lock_enforces_named_checkpoint_uniqueness` | Concurrent public checkpoint creates publish one name owner (`CAT-1`). |
| `every_internal_checkpoint_prefix_is_hidden_and_reserved` | Names beginning with `__zerofs_` are never public. |
| `exact_delete_preserves_replacement_and_blocks_unpinned_branch_origin` | Exact checkpoint deletion preserves name replacements and held sources (`ID-1`, `CAT-1`). |
| `absent_exact_retry_clears_a_crash_left_checkpoint_fence` | Lost checkpoint-delete responses settle idempotently by exact UUID (`CAT-1`). |
| `retry_forever_catalog_backend_is_bounded_by_policy_deadline` | Catalog/GC retry policy is bounded and retains on exhaustion (`GC-1`). |
| `creating_branch_with_source_hold_but_no_manifest_fails_closed` | An incomplete/unreadable source root aborts GC (`GC-1`). |
| `external_work_and_view_ceilings_fail_at_the_boundary` | Collection enforces explicit work bounds without deleting (`GC-1`). |
| `topology_index_is_linear_in_catalog_edges_at_scale` | Replacement marking work scales with roots/references, not candidate×view probes. |

Scenarios that must be retained in new tests include: crashes on both sides of
each publication/deletion point; ambiguous durable writes and deletes; exact and
conflicting retries; checkpoint deletion before/during/after clone publication;
parent/middle-ancestor deletion; mounted descendant races; name reuse; expired
lease renewal; catalog changes in every GC phase; missing/corrupt mark shards;
cutoff boundary objects; collector restart; and uncertainty-retains-data model
checks.

## Change and review policy

- Storage model, catalog, lifecycle, leases, GC, API, and integration/rollout are
  separate coherent commits (and separate pull requests when published).
- Production code and focused tests land together. A subtask is committed only
  after an independent review finds no blocker and its relevant gates pass.
- A change should normally remain below roughly 800 non-generated lines. Larger
  changes must explain why splitting would obscure an atomic invariant.
- Generated lock/proto output is reported separately from authored code.
- No commit may mark an implementation checkbox complete based only on this
  specification; the corresponding code, fault test, or operational evidence
  must exist.
