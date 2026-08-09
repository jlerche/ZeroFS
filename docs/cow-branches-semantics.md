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
plus destination namespace. Recording the exact authenticated `DurableRoot`
atomically replaces that source hold because the destination is then
independently pinned; `root_created` retains only the destination. A live branch or
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

The branch mutation RPC carries a permanent operation UUID and destination or
subject branch UUID. Creation also carries the exact source branch UUID and a
checkpoint name that SlateDB resolves once to its immutable checkpoint UUID and
root. Server time is persisted with the first reservation; retries recover that
timestamp from the operation record, including overlapping first attempts, so
clients need retain only the two UUIDs. Deletion derives the consumed branch
revision from the live record or matching permanent delete operation. The CLI
prints the exact `--id`/`--operation-id` retry values before issuing a mutation.
Both mutations commit through authoritative SlateDB before best-effort
PostgreSQL/JSON reconciliation; their response contains no storage root.

### Create and retry contract

A create request contains `operation_id`, destination UUID/name, exact source
checkpoint UUID and immutable manifest identity, and (for history) parent UUID.

- The same operation ID with identical immutable inputs returns the existing
  in-progress or completed result.
- The same operation ID with different inputs returns an operation conflict.
- A different operation targeting a reserved UUID or live name returns an
  identity/name conflict.
- The source checkpoint remains held as a GC root until an independently pinned
  destination root is recorded.
  Checkpoint deletion and this hold are serialized in one catalog mutation
  domain for that exact checkpoint/operation. After storage establishes the
  destination root, one batch replaces the source hold with that incomplete
  destination root. The `Ready` publication batch atomically converts it to the
  live branch head; there is no interval with neither.
- Publication is the transition to `Ready`, after destination-root durability.
  A response lost after publication is recovered by reading the operation and
  branch records; publication is not repeated with a different root.
- A process/object-store failure before a known durable destination root leaves
  `Creating`. Recovery retries the same idempotent storage operation or proves
  the orphan safe to remove. Ambiguity retains data.
- Creating directly from a live head is not a production API. A caller first
  creates an explicit named checkpoint, which seals and flushes the mounted
  writer, then invokes this exact checkpoint-based primitive. After independent
  destination publication the caller may logically delete that named checkpoint;
  retries continue to use its stable UUID and the destination remains valid.

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

The admin deletion request therefore carries both the stable checkpoint UUID
and its historical name. The CLI resolves the UUID before mutation and prints a
`--id` retry value if the response is ambiguous. Logical catalog deletion is the
namespace linearization point; physical SlateDB checkpoint metadata is retained
until leases and incomplete operations no longer require that exact root.

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
- A writer lease rejects deletion before `Ready` can change. The writer must
  stop, publish its immutable head, and retire through the dedicated atomic
  transition; only then may deletion consume that new ready-branch revision.
  Reader leases do not block deletion and independently retain the exact
  immutable roots from which those readers continue.
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
minutes and retains an expired read lease root for an additional 30-second
clock-skew window before an expiry mutation may tombstone it. Only a SHA-256
binding of the caller-held UUID renewal token is stored. A compact permanent
lease tombstone retains that binding and UUID so exact release/expiry retries
are idempotent and the lease incarnation can never be recreated.

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
- Generic release is idempotent for an exact read-lease UUID/token. It rejects
  a live writer, just as generic expiry does; only the head-publication batch may
  retire a writer. A read lease relies on bounded expiry after a crash. An
  expired writer loses serving authority but remains an explicit recovery
  blocker until a fenced recovery path publishes its durable head.
- Once expired or released, a lease UUID/token pair can never be resurrected.
- Clock uncertainty extends retention; it never shortens it.

A writer lease authorizes commits only while the branch remains `Ready`, and a
branch may have exactly one writer lease. Committed writes must advance the
authoritative branch head before acknowledgement. The storage and catalog
transition exists, but until the server opener invokes it at the write-response
and shutdown boundaries, the server cannot expose a writable branch mount.
Global root capture and second observation fail closed while any writer lease
record remains, including an expired crash-recovery blocker. Deletion cannot
enter `Deleting` until exact writer head reconciliation has completed.

### Mount admission and restart

A branch mount request carries the current branch name, the expected stable
branch UUID, a new lease UUID and renewal token, the requested access mode, and
a bounded duration. The name is only the first lookup key. The catalog admits
the lease only if that name still resolves to the expected UUID, the branch is
`Ready`, its revision and root are unchanged, and the root authenticates.
Immediately before returning the data-plane capability, the lifecycle verifies
the exact granted root again. The returned lease—not the requested name—is the
mount authority.

An exact retry first resolves the already durable lease by lease UUID, renewal
token binding, subject UUID, mode, duration, and root. It does not resolve the
name again. Consequently, a response lost before a process crash can be
recovered after reopening the authoritative SlateDB catalog, even if deletion
has since removed the old name and a new branch UUID has reused it. A fresh
lease request for the old UUID cannot use that reused name and fails rather
than retargeting the mount.

The server data-plane opener must consume only this stable grant. It owns lease
renewal while serving. After a writer has stopped serving, durably flushed, and
closed, the root store creates a permanent internal checkpoint named for the
exact writer lease and conditionally publishes an immutable head descriptor.
The descriptor authenticates both the admitted root's SlateDB writer epoch and
the closed head's strictly greater writer epoch; opening and closing no newer
writer is not sufficient publication evidence.
SlateDB then atomically replaces the ready branch root, exposes every private
epoch that the new head can reference, removes the writer/index and its GC
blocker, and records the exact publication in the permanent lease tombstone.
The old branch root and writer root remain authoritative until that single
batch commits. A lost response retries to the same storage checkpoint and
recorded catalog generation.

A read mount may rely on bounded expiry after a crash. A crashed writer cannot:
its retained record globally fences collection. Given the exact lease UUID,
stable branch UUID, duration, mode, and renewal token, recovery may point-read
the retained capability even after expiry, but this neither renews nor
resurrects it. A later process must still open a newer SlateDB writer
incarnation, reconcile and close it, publish that durable head through the same
transition, and only then acquire a fresh serving lease. The configured server
mount enforces this ordering before listeners start. It also confirms renewal
before serving, renews while serving, stops renewal before orderly data-database
close, and publishes the immutable head before closing catalog authority.

## Customer projection and administrative inspection

Customer lifecycle mutations and data-plane mount admission are guarded by
server-owned feature controls. Branch creation, branch mounting, checkpoint
deletion, and branch deletion default off independently when a lifecycle is
opened from `CatalogConfig`; read-only list and inspection remain available.
Direct branch/checkpoint lease acquisition carries the same mount control, so
callers cannot bypass admission through the lower-level lease API. Renewal,
release, and expiry remain available after disablement so existing leases are
not stranded. The controls authorize API entry only. They are not stored in
the catalog and cannot substitute for SlateDB revision, lease, root, or
deletion fences.

PostgreSQL and JSON are identical customer-facing projections. Both contain
reconstructible lifecycle and customer metadata, and neither is a mount, write,
or GC authority. Durable roots, manifests, active leases, storage operation
proofs, private epochs, GC guards, and permanent internal ID reservations remain
in authoritative SlateDB for local and production deployments.
Both backends expose the same bounded query contract: point lookup and stable
UUID-ordered pages, optionally restricted by resource kind, historical parent,
and customer-visible state. A page is limited to 256 records and uses the last
returned UUID as its exclusive cursor.
Deleted and compacted `absent` records remain queryable for customer audit;
pagination never consults or exposes SlateDB roots, leases, or storage proofs.
The admin RPC exposes that boundary for branches and for checkpoints on the
active branch. Catalog-backed checkpoint list/info reads return only `ready`
projection records bound to that branch; they never fall back to retained
physical SlateDB checkpoints. Legacy volumes without a catalog retain the old
local physical read behavior.
`zerofs branch list -c <config> [--after <uuid>] [--limit 1..256]` and
`zerofs branch info -c <config> <uuid>` are thin clients over those calls.
`zerofs checkpoint list -c <config> [--after <uuid>] [--limit 1..256]` exposes
the same bounded checkpoint view, while checkpoint info accepts `--id` for an
exact customer resource lookup.
The wire record contains lifecycle identity, lineage, timestamps, observed
projection generation, and customer metadata JSON; it has no field capable of
carrying a durable root, manifest, lease, renewal secret, or writer proof.

The server's optional `[catalog]` settings own a stable volume UUID, the
authoritative SlateDB path, private branch-database root, default-off lifecycle
controls, and one projection selection. `backend = "json"` uses a local file;
`backend = "postgres"` uses the same projection schema and accepts an
environment-expanded connection string. Catalog startup requires the shared
segment pool and retains the authoritative lifecycle for the serving process.
The live database, catalog, private branch root, and shared pool must occupy
pairwise-disjoint object-store namespaces.
Projection open or reconciliation failure is logged but never invalidates the
SlateDB authority; a later reconciliation can rebuild the projection.
Catalog open and projection reconciliation are the final fallible single-node
serving-assembly step, and orderly shutdown explicitly closes the catalog.
Read-only and checkpoint servers never open it. Configuration rejects combining
`[catalog]` with `[replication]`: a separate SlateDB writer cannot safely derive
catalog authority from the data-plane election because a stale late open could
fence the promoted catalog writer. HA catalog support requires catalog mutations
to share the replicated writer authority domain.

Operational inspection reads one validated SlateDB generation and returns a
bounded UUID-ordered page of one resource kind. The production maximum is 256
records per page. Administrators can inspect live branch UUIDs and roots,
leases, tombstones, and incomplete branch create/delete operations. Completed
operations are excluded from the incomplete-operation views, and renewal-token
hashes are redacted. Every page reports its catalog generation so callers can
detect changes between pages; an inspection result never grants mutation or
mount authority.

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

The immutable root stored in a writer lease cannot describe commits made after
mount admission. Consequently, a writer lease is also a global collection
fence: capture and second observation reject it as lease uncertainty rather
than marking a stale checkpoint. Read leases continue to participate as exact
immutable roots. This fence is removed only by the atomic writer-head
publication transition; generic release and elapsed wall time are not evidence
that the mutable head was reconciled.

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
- its portable content digest and physical metadata still match the exact
  first-observation object identity;
- it is not protected by an active lease, recovery record, replication root, or
  pinned GC run; and
- no relevant metadata was missing, corrupt, unreadable, unauthenticated, or
  ambiguous.

Failure to prove any condition retains the object.

After the accepted second observation, every valid root transition is
forward-closed: it may inherit segment IDs only from an authoritative root that
was already captured at that observation, or publish newly allocated IDs from a
permanent pool epoch reservation. It may never manufacture a reference to an
older unreachable segment ID. This follows from clone/source holds, active
writer and recovery roots, and pool-global non-reuse. Without that invariant,
physical deletion would require an atomic catalog-wide mutation barrier rather
than the generation preflight used here, and must fail closed.

The authority boundary enforces this rule rather than relying on caller
discipline. Production catalog mutations expose no generic branch/checkpoint
root insertion or root-replacement operation: roots are published only by the
dedicated authenticated lifecycle transitions. Checkpoint creation verifies the
complete permanent physical descriptor and unique public-name owner before
publication; branch head advancement authenticates the closed newer-writer
proof. Generic branch root-bearing mutations exist only as unit-test fixtures,
where replacement is additionally forbidden from changing the root.

Every legitimate segment writer also treats `segments/...` keys as immutable.
Small seals use conditional create. Multipart seals upload to a unique
non-authoritative staging key and atomically copy-if-absent into the segment
namespace. A lost response or exact retry succeeds only after streaming
byte-for-byte reconciliation; a different payload at the same key fails closed.
Thus an admitted writer cannot replace a candidate between GC's immediate
identity check and delete.

The legacy per-database segment reclaimer, compactor source deletion, and orphan
sweep are disabled whenever a shared segment pool is configured. Their liveness
view covers only one database and therefore cannot prove that a sibling branch
has released a segment. Metadata/tombstone GC continues independently; physical
shared-pool deletion belongs exclusively to the authoritative catalog protocol
until a future local-GC proof establishes exact private ownership.

### Fast local GC proof

Segment origin is not segment privacy. In particular, attaching a branch UUID
to an object key proves which writer allocated it, but a later checkpoint or
descendant may still reference it. Local deletion therefore requires one exact
private-epoch capability, not an ancestry walk or an owner-shaped key.

An epoch is eligible for local GC only while every condition below is proven:

1. Its permanent pool reservation is authenticated by the volume key and binds
   the epoch to the exact, never-reused branch UUID. Path, name, parent, and
   mutable branch revision are not identity. Reservation format v2 includes an
   optional authenticated branch UUID; an absent owner and every legacy v1
   reservation are valid for global uniqueness but are permanently global-only.
2. Authoritative SlateDB contains the matching private-epoch record in
   `sealed_private` state. Before that transition, the writer rotates away from
   the epoch and holds the branch-local FrameLoc/reference-publication barrier
   while it drains allocation, seal, replication, recovery, compaction, and
   other reference publishers. Once sealed, no future local mutation may
   publish a reference into that epoch. Pre-private-epoch reservations,
   missing/corrupt records, inherited epochs, and conflicting identities are
   always global-only.

Catalog schema v12 stores these epoch records as independent SlateDB keys bound
to the pool UUID, reservation UUID, branch UUID, epoch, and database identity.
Registration begins only in `open`; revision-fenced transitions are monotonic
to `sealed_private` or permanently `exposed`. Branch deletion atomically exposes
all remaining private epochs before fencing the branch. These records alone do
not authorize deletion and are absent from PostgreSQL/JSON customer projections.
SlateDB also maintains an audited branch-to-unexposed-epoch index and admits at
most 64 such epochs per branch. Registration, exposure, deletion, and writer-head
publication update the record and index in one durable batch, so branch-local
publication work is bounded rather than scanning all epochs in the catalog.

Private registration is admitted only after rereading the permanent epoch
marker and verifying its v2 HMAC with the segment-pool authority. Pool UUID,
reservation UUID, branch UUID, epoch, and database identity are copied from that
authenticated marker; caller-supplied branch and database identities must match
exactly, and that database identity must equal the ready branch's authoritative
root identity. Legacy v1 and ownerless v2 reservations remain global-only. Collection
must authenticate the marker again, so registration is not deletion authority.
An exact registration retry reconciles by immutable marker identity and original
registration time, even if deletion has already advanced the record to
`exposed`; it never requires or recreates the earlier `open` state.

Sealing is reachable only through a filesystem-issued publisher-drain receipt.
The filesystem serializes rotations, takes the FrameLoc publication barrier and
database flush barrier exclusively, drains every in-flight/background seal,
PUTs the old open segment, flushes its metadata, and installs a fresh counter
namespace in an already-reserved successor epoch before writers resume. The
catalog lifecycle used for sealing is first bound to that exact live
`ExtentStore` publisher-instance capability. It rejects a receipt from any
separately constructed store, even for the same epoch pair. It then
reauthenticates both permanent branch-bound markers, requires both records to
name the same exact ready branch/database/pool and the successor to remain
`open`, and consumes the receipt's publisher identity and exact old/new epoch
pair for one atomic transition that revision-fences both records while changing
only the old epoch to `sealed_private`. Concurrent rotations form a strict epoch chain. A crash
after rotation but before catalog sealing leaves the old epoch `open` and thus
global-only; it never guesses or reconstructs local-GC eligibility.
3. The collector holds an authoritative exclusion guard for that exact branch
   UUID, sealed epoch, and bounded batch. Guard acquisition and epoch-state
   validation are one atomic catalog transition. The guard is durable and does
   not expire or permit revocation during the batch. While it exists,
   clone-source capture, externally durable checkpoint publication, branch
   deletion, and lease/recovery-root acquisition that could expose an older
   view must conflict or wait until every attempted delete has confirmed its
   outcome and progress is durable.

Catalog schema v13 stores each acquired local-GC guard under an independent
SlateDB key. Acquisition atomically requires the exact `sealed_private` epoch
revision and ready branch, a bounded nonempty candidate count and digest, and
the absence of checkpoints, leases, incomplete clone-source operations, other
root-retaining GC runs, or another guard for the epoch. A live guard blocks
epoch exposure, branch deletion, checkpoint publication, clone-source capture,
and new branch/checkpoint leases. It has no expiry or generic release operation;
only a fully classified deletion-progress transition may account durably for
every candidate and retire it. PostgreSQL and JSON do not project guards.

Catalog schema v14 stores a separate bounded progress/audit record for each
guard. Its cursor equals the aggregate deleted-plus-already-absent count and
cannot exceed the guard's fixed candidate count. Progress revisions and
timestamps are monotonic and keep the immutable branch, epoch, count, and
candidate digest. The final fully classified update atomically writes the
completed audit and retires the guard; an incomplete update cannot retire it,
and an exact retry after an ambiguous completion response is generation-neutral.

Private candidate preparation accepts only a nonzero epoch different from the
live writer and runs under the exclusive FrameLoc-publication and database-flush
barriers. It seals the active successor before the database-wide metadata flush,
so unrelated current-epoch pointers cannot become durable before their object.
It then streams the durable exact-epoch segment counters, sorts zero-live
candidates by segment identity, and applies the fixed catalog batch bound.
Every selected object must also pass reverse-directory checks
against both the current and durable forward maps. The object is streamed in
full to bind its size, modification time, and SHA-256 content identity into the
candidate-set digest; unreadable or still-referenced objects are retained.
Preparation is not deletion authority by itself, and local deletion remains
disabled until the artifact, guard, progress, and barrier-through-delete worker
are wired as one crash-recoverable lifecycle.

The prepared descriptors can be published under an immutable
`private-gc-artifacts/<guard UUID>.bin` key. Before preparation, the live extent
store is permanently bound to one exact branch UUID and database identity. The
canonical bounded encoding binds that owner capability together with the guard
UUID, exact live publisher identity, monotonic SlateDB data-writer epoch, sealed
segment epoch, candidate digest, and every object descriptor. Conditional
create plus byte-for-byte reconciliation makes an exact
lost-response retry safe and rejects UUID reuse with different bytes. Only an
opaque receipt from that successful publication can enter guard acquisition.
The lifecycle requires the receipt's publisher, branch UUID, and database
identity to match its bound live writer and the requested branch, then
reauthenticates the permanent branch-epoch marker and exact authoritative
`sealed_private` revision before the atomic catalog mutation rechecks root
blockers. Thus same-pool credentials cannot attach branch A's local candidates
to a valid epoch owned by branch B. The artifact and guard remain
SlateDB/object-store authority and are not projected to PostgreSQL or JSON.
Recovery reads the immutable object only after rejecting an oversized metadata
length, then strictly decodes bounded fields, exact UTF-8 owner identity,
same-epoch strictly ordered segment IDs, canonical optional object identities,
the recomputed candidate digest, and the exact original canonical bytes. Wrong
guard UUIDs, truncation, trailing bytes, malformed timestamps, duplicate or
reordered candidates, and noncanonical encodings fail closed. This read-only
primitive grants no deletion authority. Physical deletion remains disabled
from normal scheduling pending full production mount/collector wiring.

The live-process worker is narrower than crash recovery. It accepts only the
opaque artifact capability issued to the same process-unique publisher that is
still bound to the extent store. For each cursor position it takes the exclusive
FrameLoc-publication barrier, rechecks serving authority, the live guard,
authenticated marker, exact sealed epoch revision, artifact owner/digest, and
both current and durable forward maps. It then streams and matches the strong
object identity, deletes, confirms absence, and publishes the next durable
cursor while retaining that same barrier. An already absent object advances as
such; any ambiguity retains it. Completion atomically retires the guard, and an
exact replay returns the completed audit.

Restart recovery uses artifact format v2's data-writer epoch. Private ownership
can be bound only to a `Db` constructed by the authenticated mount boundary
with the same immutable database identity; an ownerless or merely relabeled
database is ineligible. A recovered worker must open that exact database at a
strictly greater nonzero SlateDB manifest writer epoch, which durably fences all
metadata publication by the artifact's former writer. It then uses the same
guard/marker/epoch/forward-map/identity/barrier worker as the live path. The
worker also proves under that barrier that its current segment allocator uses a
different storage-authenticated authoritative `open` epoch for the same branch,
database, and pool; the guarded sealed epoch can never be reused for allocation.
The confirmed dead counter is removed before progress publication so an absent
object cannot monopolize later bounded batches.

A delayed former object PUT does not defeat this fence: all segment PUTs are
immutable conditional creates and cannot publish a `FrameLoc`; the newer
SlateDB writer epoch rejects the only operation that could restore reachability.
Such a PUT can at worst recreate the identical bytes as an unreferenced orphan,
which remains eligible for the global two-observation collector. If the exact
database identity or strictly newer writer epoch cannot be proven, recovery
retains the guard and every candidate.

Private collection is driven only through an explicit policy that defaults to
disabled. One invocation resumes the oldest durable guard for the exact mounted
branch/database before considering new work; otherwise it inspects a configured
bounded number of authoritative `sealed_private` epochs and can complete at
most one bounded batch. Guard acquisition atomically rechecks same-branch
checkpoints, active lease/root uncertainty, incomplete descendant creation, and
root-retaining global GC runs. A blocker or any ambiguous ownership retains the
object for global GC. Normal ownerless mounts cannot construct this coordinator,
and no normal-server configuration enables it yet.

The authoritative SlateDB backend serves private collection through targeted,
lock-consistent views. Work selection reads private epochs, active guards, and
the exact validated ready owner branch rather than materializing every catalog
collection. Its durable-root identity must equal the authenticated branch
database identity. Each candidate transition point-reads only its guard,
progress, guarded epoch, current allocator epoch, and that exact owner branch
while the filesystem publication barrier is held. Missing, malformed,
non-ready, wrong-root, or internally inconsistent targeted records retain data.

Guard admission also avoids catalog-wide root scans. Schema v15 maintains one
derived blocker record for each exact branch incarnation, counting its live
checkpoints, leases, and incomplete child creates, plus one singleton count of
root-retaining global GC runs. The root mutation and its checked counter update
share one durable SlateDB batch under the catalog mutation lock. A checkpoint
lease remains attributed to its branch through the live checkpoint or its
root-free tombstone, and branch blocker records survive logical branch deletion
so later lease release can still decrement the exact owner. Admission reads
only the requested branch record and the singleton; roots owned by another
branch do not block it. Missing, overflowing, underflowing, or audit-mismatched
records fail closed. Migration rebuilds the derived records from authoritative
roots, and complete snapshots audit them. These internal indexes are storage
authority only and are absent from the identical PostgreSQL and JSON customer
projections.

After a caller-selected retention cutoff, schema v16 may compact a full
branch/checkpoint tombstone to a permanent root-free catalog-ID reservation.
The reservation contains only the UUID and kind, so global UUID non-reuse and
exact incarnation isolation survive without retaining names, lineage, roots,
or deletion timestamps in authoritative SlateDB indefinitely. An eligible
branch tombstone atomically validates and retires its published delete-operation
UUID as another minimal reservation.

Compaction is conservative. The exact branch blocker must report no leases and
no incomplete child creation, the global blocker must report no root-retaining
GC run, checkpoint history must still identify its branch, and the retention
cutoff must have passed. Missing, malformed, or inconsistent authority retains
the full record or fails closed. A durable UUID cursor makes each pass fair
while fixed scan and mutation ceilings bound its work; one pass reports its
age/root/dependency retention and an eligible-backlog lower bound. PostgreSQL
and JSON do not receive reservation records. Both preserve the same customer
metadata and historical fields while reconciliation changes the compacted
resource from `deleted` to `absent`.

Global collector artifacts live only below `__zerofs_gc/<run UUID>/`. A
disabled-by-default cleanup call may drain that prefix only for an exact
schema-valid terminal `Reported` or `Completed` run and only after a
caller-selected retention period.
It never removes the compact catalog run/audit record. Each pass lists and
confirm-deletes at most 4096 objects; final mark, inventory, quarantine, and
revalidation shards and intermediate run/merge files are treated uniformly.
Objects newer than the retention cutoff remain. Confirmed absence reconciles
an ambiguous delete, and relisting the shrinking prefix is the crash-resumable
cursor, so a completed empty retry is a no-op. The prefix is disjoint from the
segment pool, branch databases, and authoritative catalog.

Epoch reservations are globally unique but not numerically ordered. No local-GC
decision interprets a smaller integer as older: preparation excludes the exact
active writer, and authoritative guard attachment proves the requested term was
actually rotated away and sealed.
While a create operation is incomplete, its immutable `parent_id` is required
to equal the authoritative source checkpoint's branch UUID. Snapshot validation
rechecks that binding whenever the source checkpoint is live; after checkpoint
deletion, it requires the checkpoint tombstone's preserved former branch UUID
to match the operation. Missing or mismatched migrated history fails closed.
The previously validated immutable value therefore continues to identify the
RootCreated operation as a guard blocker. A ready branch's parent remains only
historical lineage and is not a general liveness dependency.
4. No existing named/internal checkpoint or active lease/recovery root can
   expose a segment the local database now considers dead. The conservative
   implementation may pause local deletion for any such root. Unreadable or
   changing local or catalog state retains data.
5. Every candidate's segment ID carries the guarded epoch. The reclaimer may
   inspect only the exact branch database for those candidates; inherited,
   closed, shared, or otherwise ambiguous epochs remain for global GC.

Before any operation can publish a root derived from the branch, it atomically
changes every affected `open` or `sealed_private` epoch to `exposed` after
excluding local collectors and before capturing the source root. `Exposed` is
permanent. A writer rotates to a newly reserved/open epoch for later private
allocations before sealing an old one, and it must never reopen or reuse the old
epoch. If rotation and publisher drain cannot be completed, writes may continue
only in global-only mode and local deletion stays disabled.

The local collector checkpoints bounded progress and, for each object, holds
the same branch-local FrameLoc/reference-publication barrier continuously from
final local non-reference and guard validation through exact object-identity
validation, delete, and confirmed absence. This closes the final-check/delete
window even if a future publisher violates the sealed-epoch optimization.
Releasing or losing either authority stops deletion.

A crash leaves the durable batch guard in place. Recovery may resume the exact
operation only after fencing the former database writer and proving that any
remaining request cannot restore metadata reachability; it then reconciles each
candidate by exact identity or confirmed absence. The guard cannot be expired,
stolen, administratively cleared, or converted to `exposed` merely because it
is old. If fencing or ownership cannot be proven, root publication remains
excluded and the objects are retained for repair. Global two-observation GC
remains the sole collector for all other segments. This deliberately rejects
the research design's structural "owner UUID" rule as a deletion proof:
allocation ownership alone does not exclude descendant references.

### Streaming mark, inventory, quarantine, and delete

All branches of one volume resolve `FrameLoc` segment IDs in one immutable,
volume-wide segment pool, separate from every SlateDB database namespace. A
segment ID is globally unique within that pool; writer admission must allocate
its epoch by conditionally creating a permanent reservation marker in shared
pool storage rather than reuse a per-clone SlateDB writer epoch. A pool genesis
is conditionally created only while the pool is empty and is authenticated by
a subkey of the volume encryption key; every epoch marker authenticates its
exact genesis UUID, epoch, reservation UUID, and database identity. The local
counter starts at zero only after that marker succeeds. Runtime readers,
writers, replication replay, and GC use the same configured prefix. The pool
identity is captured in each GC run and cannot be changed between marking,
quarantine, revalidation, and deletion. A legacy run without this identity, a
per-branch segment namespace, or an ambiguous segment ID fails closed. This is
storage authority in SlateDB/configuration and is never copied into PostgreSQL
or JSON customer projections.

The shared segment pool is also the volume encryption-key root. New CoW volumes
establish it directly. A legacy single-database volume must instead run the
reviewed offline import below; startup rejects a retained database-local key or
legacy segment namespace unless the exact database identity has an authenticated
completion record. Every segment in an admitted pool must have a readable epoch
marker authenticated for that pool genesis and epoch. Copying shaped key,
marker, or segment objects cannot manufacture migration completion.

### Offline legacy-to-pool migration

Stop every reader, writer, mount, replica, GC collector, and maintenance process
for both the legacy source and the complete target shared pool, disable
replication in the source configuration, set a disjoint
`storage.segment_pool_path`, and run:

```text
zerofs migrate-legacy-pool --config zerofs.toml --confirm-offline
```

`--confirm-offline` is an operator assertion, not a distributed stop mechanism.
The command refuses to run without it or while replication is configured. Keep
all source and target-pool serving, GC, and maintenance processes stopped after
the command succeeds, start the migrated database normally, and resume other
pool activity only after that startup has authenticated completion and admitted
the database root. Migration intent, claims, copied segments, and completion are
not GC roots, so an active pool collector in either the copy phase or the
completion-to-root-admission window would make the procedure unsafe. The source
database and target pool must use the same object-store configuration, and
neither path may contain the other.

The first attempt binds the source path to its persistent database-instance UUID
and publishes a create-only bootstrap before touching the pool key. That marker
makes a crash before genesis fail closed instead of allowing normal startup to
reinterpret the partial pool as native. The command then unwraps the local key
with the configured password and conditionally copies its exact serialized
wrapper to the empty/new pool; an existing different wrapper is a hard conflict.
Authenticated pool genesis binds the first legacy source UUID and wrapped-key
digest. The importer streams and durably fingerprints the complete initial
`segments/` inventory and rejects every physical `Segid` already present in the
pool before publishing authenticated intent. Each imported epoch receives a
permanent authenticated reservation.
Different sources may share an epoch only when their counters—and therefore
their complete physical segment IDs—are disjoint; any duplicate physical ID is
rejected rather than rewritten.

For each source object the importer validates the canonical key against its
footer, streams a full-object SHA-256 and byte count, publishes one immutable
pool-global `Segid` ownership claim bound to the exact source UUID/key digest,
performs create-only copying without changing the `Segid` or any `FrameLoc`, and
streams the target again to prove exact identity. Pool-global create-only claims
give concurrent sources one winner even when their bytes match. A second source
inventory and streamed durable-claim traversal verify source, claims, and target
against the initial fingerprint, including a claimed object removed across a
crash, before authenticated bounded completion. The completion contains only
migration/pool/source identity, aggregate count and bytes, and a commutative
inventory fingerprint. Memory does not scale with inventory: claims remain
independently keyed and epoch validation point-checks each streamed segment's
reservation rather than retaining every distinct epoch.

A failed or response-ambiguous run is retried with the same command. It may
reconcile only targets already covered by its exact authenticated pool-global
claims; an unclaimed object appearing at a destination key fails closed.
Bootstrap, genesis, intent, database-instance UUID, and retained-key digest all
force the same completion, so removing legacy remnants or reusing the source
path cannot bypass startup. Legacy key and segment objects are retained
as an offline rollback copy and are never served once the pool is configured.
After migration, password rotation rewraps the authoritative pool key. The old
database-local wrapper may therefore differ; the completion authentication,
which derives from the unchanged DEK, proves compatibility on later startup.

Schema v17 supports a terminal shadow path after marking. `report` performs the
same cutoff-bounded physical inventory and partitioned merge join as the first
quarantine observation, then publishes immutable candidate shards, exact
candidate counts/bytes, and a `Reported` phase in one authoritative SlateDB
transition. That transition atomically releases the captured roots. It never
publishes `Quarantined`, cannot enter revalidation or deletion, and performs no
segment delete. Exact retries verify the report artifacts. PostgreSQL and JSON
remain identical customer projections and receive none of the run, root,
candidate, or report state.

1. Capture generation `G`, immutable root list/digest, and an inventory cutoff;
   pin the list for the run.
2. Enumerate each root once and emit segment IDs into memory-bounded sorted runs
   partitioned by a stable segment-ID prefix.
3. Merge/deduplicate runs into checksummed authoritative mark shards. Bloom
   filters may avoid work but never authorize deletion.
4. Stream physical inventory by the same partitions, exclude objects newer than
   the cutoff, and join with complete mark shards.
5. Persist first-observation candidates in quarantine; do not delete them.
   Each candidate carries a streamed SHA-256 of its bytes as portable strong
   identity; size, timestamp, ETag, or provider version alone is insufficient.
6. Re-read the catalog, require `G`, and accept or abort the mark. Catalog change
   aborts safely rather than attempting speculative reconciliation.
7. After the grace period, perform a second complete observation with a new
   generation fence. Durably pin that fresh root list before marking it, then
   remove every newly reachable, missing, changed, or otherwise uncertain
   candidate. This transition still performs no physical deletion.
8. Delete proven candidates in explicitly enabled batches of at most 4,096.
   Re-stream and match the strong object identity immediately before each
   delete, confirm absence, then durably checkpoint the shard/record cursor and
   aggregate counts. A crash before cursor publication replays an already
   absent object safely.

Physical deletion has two independent gates. Each call must opt in through its
bounded deletion policy, and its lifecycle must share an explicitly enabled
`GcDeletionControl`. The control is default-off and rapidly revocable: disabling
it stops the next batch, leaves the durable cursor unchanged, and does not stop
capture, marking, reporting, quarantine, or revalidation. Re-enabling resumes
from the same authoritative SlateDB progress record.

The default deletion policy is additionally conservative even while disabled:
64 objects per batch and a required 24-hour recorded revalidation grace. An
enabled caller may choose another bounded batch and minimum grace, but the
minimum cannot fall below the protocol's 390-second lease, skew, and propagation
floor. Deletion rejects a run whose durable second observation used less grace
than the active policy requires; changing policy cannot rewrite that history.

The run record contains only run UUID, generation, cutoff, segment-pool
identity, both immutable observation root-list identities/digests, mark and
candidate shard locations/checksums, bounded work statistics, phase, quarantine
or report time, and the configured grace boundary. The second observation
records only aggregate reachable/absent/retained counts and bytes, never
unbounded per-object catalog metadata.
Bounded per-kind blocker records retain the last reason and occurrence count for
missing roots, corrupt metadata, generation changes, lease uncertainty, or
storage unavailability. Missing state prevents deletion. The deletion record is
bounded to a cursor, immutable batch size, timestamps, and aggregate object/byte
counts; it never stores an unbounded per-object receipt list. Interrupted work
resumes exactly or aborts and leaks storage. Delete retries are idempotent.

### Production scale limits

SlateDB admission enforces 4,096 simultaneous live branch records (including
`Creating` and `Deleting`), 256 named checkpoints per branch, 64 active branch
or checkpoint leases attributed to one branch, 64 unexposed private epochs per
branch, and 64 historical parent edges.
Exact retries are reconciled before capacity checks, so reaching a ceiling does
not break idempotency. Logical deletion frees branch/checkpoint/lease capacity;
root-free tombstones and permanent UUID reservations do not consume live-branch
capacity. Schema migration accepts state exactly at each ceiling and repeatedly
fails without advancing the schema marker when prior state is over any ceiling.

Lineage depth is a small SlateDB-only record retained with each branch UUID. It
survives tombstone compaction and is checked atomically with branch reservation;
it is neither a GC root nor copied to PostgreSQL/JSON. Migration rejects cycles,
missing compacted ancestry, or an already over-limit lineage rather than
guessing a smaller depth.

Segment inventory has no object-count admission ceiling: counts and cursors are
`u64`, the pool is partitioned into 256 shards, and mark/inventory sorting holds
at most 8,192 records plus logarithmically many spill paths at once. Operators
therefore size the collection interval and artifact retention for their object
store rather than increasing an in-memory catalog limit. Performance
qualification at the supported branch/root envelope is a separate release gate;
exceeding an operator's time budget delays reclamation and must never authorize
partial deletion.

Root-pin extraction collects candidates once and performs one canonical
sort/deduplication; it never probes an ever-growing vector for each root. The
reproducible ignored release probe
`gc_root_capture_supported_envelope` constructs the exact 4,096-branch,
256-checkpoint-per-branch ceiling. On the 2026-08-09 qualification host it
validated 1,052,672 unique roots in 377 ms, collected/canonicalized them in
169 ms, and hashed them in 158 ms. Their canonical JSON is 117,710,537 bytes.
These figures qualify in-process root extraction only. Mark-generation object
I/O, its linearity on a representative backend, and concurrent foreground
latency remain separate open release gates; the probe deliberately does not
turn synthetic memory-store timings into those claims.

### Private fast path and cleanup

Local GC may bypass global marking only when an authenticated ownership record
proves a segment was created by one exact branch incarnation and was never
published into a checkpoint, clone, lease for another root, replication root, or
shared manifest. Ambiguous ownership falls back to global retention.

Tombstones may be compacted only after no lease, recovery operation, projection
reconciler, or retained GC generation can observe them. UUID non-reuse must
survive compaction. Reported and completed run records remain as bounded audit
state; their marks and candidate/quarantine artifacts are removed idempotently
in bounded passes after retention windows.

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
