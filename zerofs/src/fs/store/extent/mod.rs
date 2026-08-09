//! Persistent extent data store.
//!
//! The per-extent `b"extent"` key holds a small [`FrameLoc`](crate::segment::FrameLoc) pointer; the extent
//! bytes themselves live outside the LSM, in immutable `segments/` objects
//! written via [`SegmentStore`]. Writes are read-modify-write over full extents,
//! with sparse holes, all-zero elision, and a tail cache for sequential appends.
//!
//! Writes append sealed frames to an in-RAM open segment and commit the extent
//! pointer eagerly (no PUT on the write path). The open segment is PUT in the
//! background when it crosses a size threshold, and synchronously by the flush
//! path (the fsync barrier) before the metadata it references is made durable —
//! so a durable manifest never points at an un-PUT segment.

mod compact;
mod read;
mod reclaim;
mod select;
#[cfg(test)]
mod test_util;
mod write;

pub use reclaim::{ChainOutcome, PassOutcome, PassStatus};
#[allow(unused_imports)] // The standalone binary does not compile the catalog consumer.
pub(crate) use reclaim::{
    DecodedPrivateGcArtifact, PersistedPrivateGcArtifact, PreparedPrivateGcBatch,
    PrivateGcCandidateOutcome, QUIESCENT_AFTER_DEFAULT,
};

use crate::db::{Db, ExtentRefGuard, Transaction};
use crate::frame_codec::FrameCodec;
use crate::fs::inode::InodeId;
use crate::fs::key_codec::KeyCodec;
use crate::fs::lock_manager::KeyedLockManager;
use crate::fs::metrics::{SegmentFootprint, SegmentGcStats};
use crate::fs::{EXTENT_SIZE, FsError};
use crate::segment::Segid;
use crate::segment_store::SegmentStore;
use arc_swap::ArcSwap;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use foyer::{Cache, CacheBuilder};
use futures::stream::StreamExt;
use read::{READ_AHEAD_MAX_CONCURRENT, READ_AHEAD_TRACK_BYTES};
use select::{NominationSet, PairStats};
use slatedb::config::WriteOptions;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Semaphore;
use tracing::error;
use uuid::Uuid;
pub(crate) use write::SEAL_THRESHOLD;
use write::{MAX_INFLIGHT_SEALS, OpenSegment, TAIL_CACHE_BYTES};

pub(super) const PARALLEL_EXTENT_OPS: usize = 20;

const MAX_PRIVATE_DATABASE_IDENTITY_BYTES: usize = 4 * 1024;

pub(super) const ZERO_EXTENT: &[u8] = &[0u8; EXTENT_SIZE];

/// What a `write` leaves for the tail cache. Applied by the caller only after
/// the transaction commits, so the cache never runs ahead of durable state.
pub enum TailUpdate {
    Set { extent_idx: u64, data: Bytes },
    Clear,
    Keep,
}

/// Move-only evidence that the filesystem rotated away from `old_epoch` while
/// holding both publication barriers, durably flushed every old reference, and
/// installed `next_epoch` before writers resumed.
#[must_use = "the receipt must be consumed by private-epoch sealing"]
#[allow(dead_code)] // The standalone binary does not yet open the branch catalog lifecycle.
pub(crate) struct PublisherDrainReceipt {
    pub(crate) publisher_id: Uuid,
    pub(crate) old_epoch: u64,
    pub(crate) next_epoch: u64,
    _private: (),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrivatePublisherIdentity {
    pub(crate) publisher_id: Uuid,
    pub(crate) data_writer_epoch: u64,
    pub(crate) branch_id: Uuid,
    pub(crate) database_identity: String,
}

struct RotatingSegmentStore {
    current: ArcSwap<SegmentStore>,
}

impl RotatingSegmentStore {
    fn new(current: Arc<SegmentStore>) -> Self {
        Self {
            current: ArcSwap::from(current),
        }
    }

    fn load(&self) -> Arc<SegmentStore> {
        self.current.load_full()
    }

    #[allow(dead_code)] // The standalone binary does not yet open the branch catalog lifecycle.
    fn store(&self, next: Arc<SegmentStore>) {
        self.current.store(next);
    }

    #[cfg(test)]
    async fn list_segments(&self) -> Result<Vec<Segid>, crate::segment_store::SegmentStoreError> {
        self.load().list_segments().await
    }

    #[cfg(test)]
    fn read_calls(&self) -> u64 {
        self.load().read_calls()
    }

    #[cfg(test)]
    async fn delete_segment(
        &self,
        segid: Segid,
    ) -> Result<(), crate::segment_store::SegmentStoreError> {
        self.load().delete_segment(segid).await
    }
}

/// Human-readable byte size for log lines, e.g. "3.1 GiB". Display-only.
pub(crate) fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}

#[derive(Clone)]
pub struct ExtentStore {
    db: Arc<Db>,
    key_codec: Arc<KeyCodec>,
    segments: Arc<RotatingSegmentStore>,
    /// Process-unique capability shared by clones of this exact live writer.
    /// A separately constructed store over the same epoch pair gets a different
    /// identity and cannot satisfy the catalog's publisher binding.
    publisher_id: Arc<Uuid>,
    private_owner: Arc<std::sync::OnceLock<(Uuid, String)>>,
    /// Same per-inode write lock the foreground path uses, so the coalescer's
    /// conditional swap can't be clobbered by a concurrent write.
    lock_manager: Arc<KeyedLockManager<InodeId>>,
    codec: Arc<FrameCodec>,
    open: Arc<Mutex<OpenSegment>>,
    /// Writers hold the read side from FrameLoc assignment through commit; GC
    /// takes the write side before sealing and choosing its cutoff.
    extent_ref_barrier: Arc<tokio::sync::RwLock<()>>,
    /// Serializes appends through threshold-triggered rotation. The writer that
    /// crosses the threshold keeps this gate while waiting for a seal permit,
    /// so later writers cannot keep extending an overdue open segment.
    append_gate: Arc<tokio::sync::Mutex<()>>,
    /// Finalized bytes of segments whose PUT is in flight (or failed and pending a
    /// re-PUT). Reads consult these before the object store. Ordered so the
    /// barrier's re-PUT sequence is deterministic (seal order).
    sealing: Arc<Mutex<BTreeMap<Segid, Bytes>>>,
    /// Permits = max in-flight seals; acquiring all is the fsync drain barrier.
    seal_sem: Arc<Semaphore>,
    /// Deadline after which each currently-dead segment may be deleted:
    /// recorded the first pass it's seen dead, from the latest expiry of the
    /// checkpoints active then, so reclamation outlasts anything that could
    /// still reference it.
    delete_at: Arc<Mutex<HashMap<Segid, DateTime<Utc>>>>,
    /// Read-nominated compaction hints (see [`NominationSet`]): pushed by the
    /// demand-read path, drained by each reclaim pass to prioritize selection.
    nominations: Arc<Mutex<NominationSet>>,
    /// Whether reads nominate at all. Set once when segment GC starts.
    nominations_enabled: Arc<AtomicBool>,
    /// Crossing-pair heat (see [`PairStats`]): bumped by demand reads that
    /// fetch adjacent file data across two on-store segments, read by each
    /// reclaim pass to form chain-compaction selections.
    pair_stats: Arc<Mutex<PairStats>>,
    /// Monotone reclaim-pass counter: the clock for pair-stat episode dedup
    /// (staleness is wall-clock). Bumped once per pass, pinned passes included.
    gc_round: Arc<std::sync::atomic::AtomicU64>,
    /// Write-quiescence tracking: the (epoch, cutoff) the previous pass saw
    /// and the monotonic instant it was first seen unchanged. Touched only by
    /// the single segment-gc task.
    quiescence: Arc<Mutex<(u64, u64, Instant)>>,
    /// Per-inode copy of the most-recently-written (extent_idx, full extent), so a
    /// sequential append splices into it rather than re-decoding the buffered/sealed
    /// frame. Eviction only ever costs a re-fetch.
    tail_cache: Cache<InodeId, (u64, Bytes)>,
    /// Per-inode logical read-ahead state: (last_read_end, prefetched_to, seq_run).
    read_ahead: Cache<InodeId, (u64, u64, u32)>,
    /// Global bound on concurrent read-ahead fetches.
    prefetch_sem: Arc<Semaphore>,
    /// Buffer size that triggers a background seal (i.e. the segment object size).
    /// Tests and the DST harness lower it at construction time so seal paths do
    /// not allocate 256 MiB.
    seal_threshold: usize,
    /// Weak handle to the commit worker, injected post-construction (the worker owns
    /// an `ExtentStore` clone, so a strong handle would cycle). Set in production;
    /// unset in the extent unit tests, where `commit_via_coordinator` is the sole
    /// segcount writer and commits directly.
    coordinator: Arc<std::sync::OnceLock<crate::fs::write_coordinator::WeakWriteCoordinator>>,
    /// Reclaim/compaction counters and footprint gauges, bridged to Prometheus.
    /// Written only by the segment-GC task (see `reclaim_segments_gated`).
    segment_gc_stats: Arc<SegmentGcStats>,
}

impl ExtentStore {
    pub fn new(
        db: Arc<Db>,
        key_codec: Arc<KeyCodec>,
        segments: Arc<SegmentStore>,
        lock_manager: Arc<KeyedLockManager<InodeId>>,
        seal_threshold: usize,
    ) -> Self {
        let tail_cache = CacheBuilder::new(TAIL_CACHE_BYTES)
            .with_weighter(|_id: &InodeId, (_idx, data): &(u64, Bytes)| data.len())
            .build();
        let read_ahead = CacheBuilder::new(READ_AHEAD_TRACK_BYTES)
            .with_weighter(|_: &InodeId, _: &(u64, u64, u32)| 24)
            .build();
        let codec = segments.codec();
        let open = Arc::new(Mutex::new(OpenSegment {
            segid: segments.next_segid(),
            buf: Vec::new(),
            dir: Vec::new(),
        }));
        Self {
            db,
            key_codec,
            segments: Arc::new(RotatingSegmentStore::new(segments)),
            publisher_id: Arc::new(Uuid::new_v4()),
            private_owner: Arc::new(std::sync::OnceLock::new()),
            lock_manager,
            codec,
            open,
            extent_ref_barrier: Arc::new(tokio::sync::RwLock::new(())),
            append_gate: Arc::new(tokio::sync::Mutex::new(())),
            sealing: Arc::new(Mutex::new(BTreeMap::new())),
            seal_sem: Arc::new(Semaphore::new(MAX_INFLIGHT_SEALS)),
            delete_at: Arc::new(Mutex::new(HashMap::new())),
            nominations: Arc::new(Mutex::new(NominationSet::default())),
            nominations_enabled: Arc::new(AtomicBool::new(false)),
            pair_stats: Arc::new(Mutex::new(PairStats::default())),
            gc_round: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            quiescence: Arc::new(Mutex::new((0, 0, Instant::now()))),
            tail_cache,
            read_ahead,
            prefetch_sem: Arc::new(Semaphore::new(READ_AHEAD_MAX_CONCURRENT)),
            seal_threshold,
            coordinator: Arc::new(std::sync::OnceLock::new()),
            segment_gc_stats: Arc::new(SegmentGcStats::default()),
        }
    }

    /// Reclaim/compaction metrics holder, for the Prometheus bridge.
    pub fn segment_gc_stats(&self) -> Arc<SegmentGcStats> {
        Arc::clone(&self.segment_gc_stats)
    }

    fn segment_store(&self) -> Arc<SegmentStore> {
        self.segments.load()
    }

    pub(crate) fn publisher_id(&self) -> Uuid {
        *self.publisher_id
    }

    #[allow(dead_code)] // The standalone server remains ownerless/global-only.
    pub(crate) fn bind_private_owner(
        &self,
        branch_id: Uuid,
        database_identity: String,
    ) -> Result<(), FsError> {
        if branch_id.is_nil()
            || database_identity.is_empty()
            || database_identity.len() > MAX_PRIVATE_DATABASE_IDENTITY_BYTES
            || database_identity.chars().any(char::is_control)
            || self.db.database_identity() != Some(database_identity.as_str())
        {
            return Err(FsError::InvalidArgument);
        }
        match self
            .private_owner
            .set((branch_id, database_identity.clone()))
        {
            Ok(()) => Ok(()),
            Err(_) if self.private_owner.get() == Some(&(branch_id, database_identity)) => Ok(()),
            Err(_) => Err(FsError::InvalidArgument),
        }
    }

    pub(crate) fn private_publisher_identity(&self) -> Option<PrivatePublisherIdentity> {
        let (branch_id, database_identity) = self.private_owner.get()?.clone();
        Some(PrivatePublisherIdentity {
            publisher_id: self.publisher_id(),
            data_writer_epoch: self.db.writer_epoch().filter(|epoch| *epoch != 0)?,
            branch_id,
            database_identity,
        })
    }

    #[allow(dead_code)] // The standalone binary omits the catalog coordinator.
    pub(crate) fn check_private_gc_serving_authority(&self) -> Result<(), FsError> {
        self.db.check_serving_authority()
    }

    /// Segment epoch currently used for new allocations. Callers that need a
    /// stable value must hold the private-GC publication barrier.
    #[allow(dead_code)] // The standalone binary omits the catalog coordinator.
    pub(crate) fn private_writer_segment_epoch(&self) -> Option<u64> {
        self.private_owner.get()?;
        Some(self.segment_store().epoch())
    }

    /// Rotate allocation to a previously reserved epoch while holding the
    /// complete FrameLoc publication and database flush barriers.
    #[allow(dead_code)] // The standalone binary does not yet open the branch catalog lifecycle.
    pub(crate) async fn rotate_writer_epoch(
        &self,
        next_epoch: u64,
    ) -> Result<PublisherDrainReceipt, FsError> {
        if next_epoch == 0 {
            return Err(FsError::InvalidArgument);
        }
        let _refs = self.extent_ref_barrier.clone().write_owned().await;
        let _flush = self.db.flush_barrier().write_owned().await;
        let _append = self.append_gate.lock().await;
        let current = self.segment_store();
        let old_epoch = current.epoch();
        if next_epoch == old_epoch {
            return Err(FsError::InvalidArgument);
        }
        self.seal_open().await?;
        self.db.flush().await.map_err(|_| FsError::IoError)?;
        let next = Arc::new(current.rotated(next_epoch));
        {
            let mut open = self.open.lock().unwrap();
            debug_assert!(open.dir.is_empty() && open.buf.is_empty());
            open.segid = next.next_segid();
        }
        self.segments.store(next);
        Ok(PublisherDrainReceipt {
            publisher_id: self.publisher_id(),
            old_epoch,
            next_epoch,
            _private: (),
        })
    }

    async fn new_extent_ref_guard(&self) -> ExtentRefGuard {
        Arc::new(self.extent_ref_barrier.clone().read_owned().await)
    }

    /// Attach one publication guard before assigning any FrameLoc.
    pub(super) async fn protect_extent_ref(&self, txn: &mut Transaction) {
        if !txn.has_extent_ref_guard() {
            txn.hold_extent_ref_guard(self.new_extent_ref_guard().await);
        }
    }

    /// One-time footprint scan: sums the segcount rows into the aggregate the
    /// monitor gauges track. Used to seed those gauges at open (after which they
    /// are maintained incrementally off the commit path) and as the ground data
    /// the incremental path is tested against.
    pub async fn sample_footprint(&self) -> Result<SegmentFootprint, FsError> {
        let (sc_start, sc_end) = self.key_codec.segcount_prefix_range();
        let mut stream = self.db.scan(sc_start..sc_end).await.map_err(|e| {
            error!("segment footprint scan failed: {}", e);
            FsError::IoError
        })?;
        let (mut segment_count, mut live_bytes, mut appended_bytes) = (0u64, 0u64, 0u64);
        while let Some(result) = stream.next().await {
            let (key, value) = result.map_err(|_| FsError::IoError)?;
            if self.key_codec.parse_segcount_key(&key).is_none() {
                continue;
            }
            let Some((live, total)) = KeyCodec::decode_segcount(&value) else {
                continue;
            };
            segment_count += 1;
            live_bytes += live;
            appended_bytes += total;
        }
        Ok(SegmentFootprint {
            segment_count,
            appended_bytes,
            live_bytes,
            reclaimable_bytes: appended_bytes.saturating_sub(live_bytes),
        })
    }

    /// Seed the monitor footprint gauges from a one-time scan. Call at store
    /// open, before writes begin, so the incremental deltas start from the
    /// existing on-store footprint.
    pub async fn seed_footprint(&self) -> Result<(), FsError> {
        let f = self.sample_footprint().await?;
        self.segment_gc_stats.seed_footprint(&f);
        Ok(())
    }

    /// Bytes held in RAM, not yet PUT to the object store: the open write
    /// buffer plus any sealed segments whose PUT is still in flight. This is
    /// the write-back buffer, the recently-written data a crash would lose
    /// without a flush. Read fresh (it is volatile); cheap in-memory lengths.
    pub fn unflushed_bytes(&self) -> u64 {
        let open = self.open.lock().unwrap().buf.len() as u64;
        let sealing: u64 = self
            .sealing
            .lock()
            .unwrap()
            .values()
            .map(|b| b.len() as u64)
            .sum();
        open + sealing
    }

    /// Inject the commit worker's weak handle so this store's GC/compaction
    /// seg-delta txns route through the single writer. Idempotent.
    pub fn set_coordinator(&self, coord: crate::fs::write_coordinator::WeakWriteCoordinator) {
        let _ = self.coordinator.set(coord);
    }

    /// Enable read-path compaction nominations.
    pub fn enable_nominations(&self) {
        self.nominations_enabled.store(true, Ordering::Relaxed);
    }

    /// No seal PUT in flight or pending re-PUT. Fast passes must not queue
    /// the barrier's all-permits drain behind a seal burst.
    pub fn seals_quiet(&self) -> bool {
        self.seal_sem.available_permits() == MAX_INFLIGHT_SEALS
            && self.sealing.lock().unwrap().is_empty()
    }

    /// Commit a txn that may carry seg-count deltas. In production this hands off to
    /// the commit worker (the sole segcount writer); with no coordinator (unit tests)
    /// we are the only writer, so materialize the deltas and commit directly.
    async fn commit_via_coordinator(&self, mut txn: Transaction) -> Result<(), FsError> {
        if let Some(coord) = self.coordinator.get() {
            return coord.commit(txn).await;
        }
        // Unit-test fallback: retain the same publication lifetime the real
        // coordinator carries across its merged database write.
        let extent_ref_guard = txn.take_extent_ref_guard();
        let deltas = txn.take_seg_deltas();
        let mut batch = txn.into_inner();
        let (_, footprint_delta) =
            crate::fs::write_coordinator::stage_seg_deltas(&self.db, deltas, &mut batch).await?;
        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| FsError::IoError)?;
        // Committed: fold the batch's net footprint into the monitor gauges,
        // mirroring the write coordinator's apply on its own path.
        self.segment_gc_stats.apply_footprint_delta(
            footprint_delta.d_segments,
            footprint_delta.d_appended,
            footprint_delta.d_live,
        );
        drop(extent_ref_guard);
        Ok(())
    }

    #[cfg(test)]
    #[allow(dead_code)] // The standalone binary omits the catalog integration test.
    pub(crate) async fn commit_test_transaction(&self, txn: Transaction) -> Result<(), FsError> {
        self.commit_via_coordinator(txn).await
    }

    /// Test-only: lower the seal threshold so seal-path tests don't build a full
    /// 256 MiB segment.
    #[cfg(test)]
    fn with_seal_threshold(mut self, n: usize) -> Self {
        self.seal_threshold = n;
        self
    }

    pub(super) fn seal_threshold(&self) -> usize {
        self.seal_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::*;
    use super::*;

    #[tokio::test]
    async fn writer_epoch_rotation_drains_old_publications_before_switching() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, b"old epoch").await;
        let old = frameloc_of(&store, &db, 1, 0).await.unwrap();
        assert_eq!(old.segid.epoch, 7);

        let receipt = store.rotate_writer_epoch(8).await.unwrap();
        assert_eq!(receipt.old_epoch, 7);
        assert_eq!(receipt.next_epoch, 8);
        assert_eq!(store.unflushed_bytes(), 0);

        write_and_check(&store, &db, &mut model, EXTENT_SIZE, b"new epoch").await;
        let new = frameloc_of(&store, &db, 1, 1).await.unwrap();
        assert_eq!(new.segid.epoch, 8);
        assert_eq!(frameloc_of(&store, &db, 1, 0).await.unwrap(), old);
        store.seal_open().await.unwrap();
        assert_read_matches(&store, &model).await;

        let hold = store.extent_ref_barrier.clone().read_owned().await;
        let first_store = store.clone();
        let first = tokio::spawn(async move { first_store.rotate_writer_epoch(9).await.unwrap() });
        let second_store = store.clone();
        let second =
            tokio::spawn(async move { second_store.rotate_writer_epoch(10).await.unwrap() });
        tokio::task::yield_now().await;
        drop(hold);
        let receipts = [first.await.unwrap(), second.await.unwrap()];
        let initial = receipts
            .iter()
            .find(|receipt| receipt.old_epoch == 8)
            .unwrap();
        let later = receipts
            .iter()
            .find(|receipt| receipt.old_epoch != 8)
            .unwrap();
        assert_eq!(later.old_epoch, initial.next_epoch);
        db.close().await.unwrap();
    }

    // The footprint gauges are maintained incrementally off the commit path, so
    // they must track writes and overwrites with no reclaim pass, and always
    // agree with an authoritative scan of the same state.
    #[tokio::test]
    async fn footprint_gauges_track_writes_incrementally() {
        let (store, db) = make().await;
        let inode: InodeId = 1;
        use std::sync::atomic::Ordering::Relaxed;
        let m = store.segment_gc_stats();
        assert_eq!(m.appended_bytes.load(Relaxed), 0);
        assert_eq!(m.segment_count.load(Relaxed), 0);

        // Write 3 extents: appended + live grow, all live, no reclaim pass.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![7u8; 3 * EXTENT_SIZE]),
                0,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        let appended = m.appended_bytes.load(Relaxed);
        assert!(appended > 0);
        assert_eq!(m.live_bytes.load(Relaxed), appended, "all live");
        assert_eq!(m.reclaimable_bytes.load(Relaxed), 0);
        assert_eq!(m.segment_count.load(Relaxed), 1);
        // The incremental gauges match an authoritative scan of the same state.
        let f = store.sample_footprint().await.unwrap();
        assert_eq!(f.appended_bytes, appended);
        assert_eq!(f.live_bytes, m.live_bytes.load(Relaxed));
        assert_eq!(f.segment_count, 1);

        // Overwrite extent 0: a new frame is appended and the old one becomes
        // dead weight, visible immediately with no reclaim pass.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![2u8; EXTENT_SIZE]),
                3 * EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        assert!(
            m.appended_bytes.load(Relaxed) > appended,
            "new frame appended"
        );
        assert!(m.reclaimable_bytes.load(Relaxed) > 0, "old frame now dead");
        assert!(m.live_bytes.load(Relaxed) < m.appended_bytes.load(Relaxed));
        let f = store.sample_footprint().await.unwrap();
        assert_eq!(f.appended_bytes, m.appended_bytes.load(Relaxed));
        assert_eq!(f.live_bytes, m.live_bytes.load(Relaxed));
        assert_eq!(f.reclaimable_bytes, m.reclaimable_bytes.load(Relaxed));
    }
}
