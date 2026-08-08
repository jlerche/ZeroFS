use super::gc_mark::{GcMarkError, MarkReader, decode_digest, encode_digest};
use super::{
    GcInventoryStats, GcMarkShard, GcQuarantineShard, GcRevalidationRecord, GcRevalidationStats,
    GcRunRecord, SlateDbRootStore,
};
use crate::segment::Segid;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::buffered::{BufReader, BufWriter};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

const INVENTORY_MAGIC: &[u8; 8] = b"ZFGCIN01";
const QUARANTINE_MAGIC: &[u8; 8] = b"ZFGCQR01";
const FILE_VERSION: u32 = 2;
const HEADER_LEN: usize = 8 + 4 + 16 + 32 + 1;
const RECORD_LEN: usize = 16 + 8 + 8 + 4 + 32;
const FOOTER_LEN: usize = 8 + 8 + 32;
const INVENTORY_BUFFER_OBJECTS: usize = 8_192;
const IO_BUFFER_BYTES: usize = 256 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct InventoryRecord {
    segid: Segid,
    size: u64,
    modified_seconds: i64,
    modified_nanos: u32,
    content_digest: [u8; 32],
}

impl InventoryRecord {
    fn from_meta(segid: Segid, meta: &ObjectMeta) -> Self {
        Self {
            segid,
            size: meta.size,
            modified_seconds: meta.last_modified.timestamp(),
            modified_nanos: meta.last_modified.timestamp_subsec_nanos(),
            content_digest: [0; 32],
        }
    }

    fn with_content_digest(mut self, content_digest: [u8; 32]) -> Self {
        self.content_digest = content_digest;
        self
    }

    fn metadata_matches(self, other: Self) -> bool {
        self.segid == other.segid
            && self.size == other.size
            && self.modified_seconds == other.modified_seconds
            && self.modified_nanos == other.modified_nanos
    }

    fn modified_at(self) -> Result<DateTime<Utc>, GcInventoryError> {
        DateTime::from_timestamp(self.modified_seconds, self.modified_nanos).ok_or_else(|| {
            GcInventoryError::Corrupt("inventory object has an invalid timestamp".to_string())
        })
    }
}

pub(crate) struct GcInventoryBuild {
    pub(crate) shards: Vec<GcQuarantineShard>,
    pub(crate) stats: GcInventoryStats,
}

pub(crate) struct GcRevalidationBuild {
    pub(crate) shards: Vec<GcQuarantineShard>,
    pub(crate) stats: GcRevalidationStats,
}

pub(crate) struct GcDeleteBatch {
    pub(crate) next_shard: u16,
    pub(crate) next_record: u64,
    pub(crate) deleted_objects: u64,
    pub(crate) deleted_bytes: u64,
    pub(crate) already_absent: u64,
}

pub(crate) struct GcInventoryStore {
    object_store: Arc<dyn ObjectStore>,
}

impl GcInventoryStore {
    pub(crate) fn new(roots: &SlateDbRootStore) -> Self {
        Self {
            object_store: roots.object_store(),
        }
    }

    pub(crate) async fn build(
        &self,
        run: &GcRunRecord,
    ) -> Result<GcInventoryBuild, GcInventoryError> {
        if run.segment_pool.is_empty() {
            return Err(GcInventoryError::Corrupt(
                "legacy GC run has no immutable segment-pool identity".to_string(),
            ));
        }
        if run.mark_shards.len() != 256 {
            return Err(GcInventoryError::Corrupt(
                "GC run has no complete authoritative mark set".to_string(),
            ));
        }
        let digest = decode_digest(&run.root_digest)?;
        let mut stats = GcInventoryStats {
            objects_seen: 0,
            objects_newer_than_cutoff: 0,
            reachable_objects: 0,
            candidate_objects: 0,
            candidate_bytes: 0,
            intermediate_runs: 0,
        };
        let mut quarantine = Vec::with_capacity(256);
        for shard in 0u8..=u8::MAX {
            let inventory = self.enumerate_shard(run, digest, shard, &mut stats).await?;
            quarantine.push(
                self.join_shard(run, digest, shard, &inventory, &mut stats)
                    .await?,
            );
        }
        self.verify_all(run, digest, &quarantine).await?;
        Ok(GcInventoryBuild {
            shards: quarantine,
            stats,
        })
    }

    async fn enumerate_shard(
        &self,
        run: &GcRunRecord,
        digest: [u8; 32],
        shard: u8,
        stats: &mut GcInventoryStats,
    ) -> Result<Path, GcInventoryError> {
        let prefix = segment_prefix(&run.segment_pool, shard);
        let mut listing = self.object_store.list(Some(&prefix));
        let mut buffer = Vec::with_capacity(INVENTORY_BUFFER_OBJECTS);
        let mut sequence = 0u64;
        let mut runs = OnlineRuns::default();
        while let Some(meta) = listing.next().await {
            let meta = meta?;
            let segid = parse_segment_path(&run.segment_pool, shard, &meta.location)?;
            stats.objects_seen = checked_add(stats.objects_seen, 1, "inventory object count")?;
            let record = InventoryRecord::from_meta(segid, &meta);
            if record.modified_at()? > run.inventory_cutoff {
                stats.objects_newer_than_cutoff = checked_add(
                    stats.objects_newer_than_cutoff,
                    1,
                    "newer inventory object count",
                )?;
                continue;
            }
            buffer.push(record);
            if buffer.len() >= INVENTORY_BUFFER_OBJECTS {
                self.flush_inventory(
                    run.id,
                    digest,
                    shard,
                    sequence,
                    &mut buffer,
                    &mut runs,
                    stats,
                )
                .await?;
                sequence = checked_add(sequence, 1, "inventory sequence")?;
            }
        }
        if !buffer.is_empty() {
            self.flush_inventory(
                run.id,
                digest,
                shard,
                sequence,
                &mut buffer,
                &mut runs,
                stats,
            )
            .await?;
        }
        let final_path = inventory_final_path(run.id, shard);
        merge_inventory_files(
            Arc::clone(&self.object_store),
            run.id,
            digest,
            shard,
            &runs.into_paths(),
            &final_path,
        )
        .await?;
        Ok(final_path)
    }

    #[allow(clippy::too_many_arguments)]
    async fn flush_inventory(
        &self,
        run_id: Uuid,
        digest: [u8; 32],
        shard: u8,
        sequence: u64,
        buffer: &mut Vec<InventoryRecord>,
        runs: &mut OnlineRuns,
        stats: &mut GcInventoryStats,
    ) -> Result<(), GcInventoryError> {
        buffer.sort_unstable_by_key(|record| record.segid);
        if buffer.windows(2).any(|pair| pair[0].segid == pair[1].segid) {
            return Err(GcInventoryError::Corrupt(
                "physical inventory listed one segment identity more than once".to_string(),
            ));
        }
        let path = inventory_run_path(run_id, shard, sequence);
        write_inventory_file(
            Arc::clone(&self.object_store),
            &path,
            run_id,
            digest,
            shard,
            buffer,
        )
        .await?;
        buffer.clear();
        stats.intermediate_runs = checked_add(
            stats.intermediate_runs,
            1,
            "inventory intermediate run count",
        )?;
        self.insert_run(run_id, digest, shard, sequence, path, runs)
            .await
    }

    async fn insert_run(
        &self,
        run_id: Uuid,
        digest: [u8; 32],
        shard: u8,
        sequence: u64,
        mut carry: Path,
        runs: &mut OnlineRuns,
    ) -> Result<(), GcInventoryError> {
        let mut level = 0usize;
        loop {
            let Some((left, right)) = runs.add_or_pair(level, carry) else {
                return Ok(());
            };
            let output = inventory_online_path(run_id, shard, level, sequence);
            merge_inventory_files(
                Arc::clone(&self.object_store),
                run_id,
                digest,
                shard,
                &[left, right],
                &output,
            )
            .await?;
            carry = output;
            level = level.checked_add(1).ok_or_else(|| {
                GcInventoryError::Corrupt("inventory merge level overflow".to_string())
            })?;
        }
    }

    async fn join_shard(
        &self,
        run: &GcRunRecord,
        digest: [u8; 32],
        shard: u8,
        inventory_path: &Path,
        stats: &mut GcInventoryStats,
    ) -> Result<GcQuarantineShard, GcInventoryError> {
        let descriptor = &run.mark_shards[shard as usize];
        if descriptor.shard != shard {
            return Err(GcInventoryError::Corrupt(
                "mark descriptors are not ordered by shard".to_string(),
            ));
        }
        let mut marks = MarkReader::open(
            Arc::clone(&self.object_store),
            &Path::from(descriptor.location.clone()),
            run.id,
            digest,
            shard,
        )
        .await?;
        let mut inventory = InventoryReader::open(
            Arc::clone(&self.object_store),
            inventory_path,
            run.id,
            digest,
            shard,
        )
        .await?;
        let output = quarantine_path(run.id, shard);
        let mut writer = InventoryWriter::new(
            Arc::clone(&self.object_store),
            output.clone(),
            QUARANTINE_MAGIC,
            run.id,
            digest,
            shard,
        )
        .await?;
        let mut mark = marks.next().await?;
        while let Some(record) = inventory.next().await? {
            while mark.is_some_and(|value| value < record.segid) {
                mark = marks.next().await?;
            }
            if mark == Some(record.segid) {
                stats.reachable_objects = checked_add(
                    stats.reachable_objects,
                    1,
                    "reachable inventory object count",
                )?;
                mark = marks.next().await?;
            } else {
                let path = Path::from(format!(
                    "{}/{}",
                    run.segment_pool,
                    record.segid.object_key()
                ));
                let identity = self.read_identity(&path, record.segid).await?;
                if !record.metadata_matches(identity) {
                    return Err(GcInventoryError::Corrupt(format!(
                        "candidate {:?} changed during first observation",
                        record.segid
                    )));
                }
                writer.push(identity).await?;
                stats.candidate_objects =
                    checked_add(stats.candidate_objects, 1, "quarantine candidate count")?;
                stats.candidate_bytes = checked_add(
                    stats.candidate_bytes,
                    record.size,
                    "quarantine candidate byte count",
                )?;
            }
        }
        inventory.finish().await?;
        while marks.next().await?.is_some() {}
        let mark_result = marks.finish().await?;
        if mark_result.count != descriptor.segment_count
            || encode_digest(mark_result.checksum) != descriptor.checksum
        {
            return Err(GcInventoryError::Corrupt(format!(
                "mark shard {shard} disagrees with its descriptor"
            )));
        }
        let result = writer.finish().await?;
        Ok(GcQuarantineShard {
            shard,
            location: output.to_string(),
            checksum: encode_digest(result.checksum),
            candidate_count: result.count,
            candidate_bytes: result.bytes,
        })
    }

    pub(crate) async fn verify_all(
        &self,
        run: &GcRunRecord,
        digest: [u8; 32],
        shards: &[GcQuarantineShard],
    ) -> Result<(), GcInventoryError> {
        self.verify_shards(run.id, digest, shards).await
    }

    pub(crate) async fn revalidate(
        &self,
        run: &GcRunRecord,
        observation: &GcRevalidationRecord,
        mark_shards: &[GcMarkShard],
    ) -> Result<GcRevalidationBuild, GcInventoryError> {
        let original_digest = decode_digest(&run.root_digest)?;
        let digest = decode_digest(&observation.root_digest)?;
        self.verify_shards(run.id, original_digest, &run.quarantine_shards)
            .await?;
        if mark_shards.len() != 256 {
            return Err(GcInventoryError::Corrupt(
                "second observation mark set is incomplete".to_string(),
            ));
        }
        let first_observation_candidates = run
            .quarantine_shards
            .iter()
            .try_fold(0u64, |total, shard| {
                checked_add(total, shard.candidate_count, "first candidate count")
            })?;
        let mut stats = GcRevalidationStats {
            first_observation_candidates,
            became_reachable: 0,
            already_absent: 0,
            retained_candidates: 0,
            retained_bytes: 0,
        };
        let mut shards = Vec::with_capacity(256);
        for shard in 0u8..=u8::MAX {
            let original = &run.quarantine_shards[shard as usize];
            let marks = &mark_shards[shard as usize];
            if original.shard != shard || marks.shard != shard {
                return Err(GcInventoryError::Corrupt(
                    "revalidation descriptors are not ordered by shard".to_string(),
                ));
            }
            let mut candidates = InventoryReader::open_with_magic(
                Arc::clone(&self.object_store),
                &Path::from(original.location.clone()),
                QUARANTINE_MAGIC,
                run.id,
                original_digest,
                shard,
            )
            .await?;
            let mut marks_reader = MarkReader::open(
                Arc::clone(&self.object_store),
                &Path::from(marks.location.clone()),
                observation.id,
                digest,
                shard,
            )
            .await?;
            let output = revalidation_path(run.id, observation.id, shard);
            let mut writer = InventoryWriter::new(
                Arc::clone(&self.object_store),
                output.clone(),
                QUARANTINE_MAGIC,
                observation.id,
                digest,
                shard,
            )
            .await?;
            let mut mark = marks_reader.next().await?;
            while let Some(record) = candidates.next().await? {
                while mark.is_some_and(|value| value < record.segid) {
                    mark = marks_reader.next().await?;
                }
                if mark == Some(record.segid) {
                    stats.became_reachable =
                        checked_add(stats.became_reachable, 1, "newly reachable candidate count")?;
                    mark = marks_reader.next().await?;
                    continue;
                }
                let path = Path::from(format!(
                    "{}/{}",
                    run.segment_pool,
                    record.segid.object_key()
                ));
                match self.read_identity(&path, record.segid).await {
                    Ok(identity) => {
                        if identity != record {
                            return Err(GcInventoryError::Corrupt(format!(
                                "candidate {:?} changed between observations",
                                record.segid
                            )));
                        }
                        writer.push(record).await?;
                        stats.retained_candidates =
                            checked_add(stats.retained_candidates, 1, "retained candidate count")?;
                        stats.retained_bytes = checked_add(
                            stats.retained_bytes,
                            record.size,
                            "retained candidate bytes",
                        )?;
                    }
                    Err(GcInventoryError::ObjectStore(object_store::Error::NotFound {
                        ..
                    })) => {
                        stats.already_absent =
                            checked_add(stats.already_absent, 1, "already absent candidate count")?;
                    }
                    Err(error) => return Err(error),
                }
            }
            candidates.finish().await?;
            while marks_reader.next().await?.is_some() {}
            let mark_result = marks_reader.finish().await?;
            if mark_result.count != marks.segment_count
                || encode_digest(mark_result.checksum) != marks.checksum
            {
                return Err(GcInventoryError::Corrupt(format!(
                    "second observation mark shard {shard} disagrees with its descriptor"
                )));
            }
            let result = writer.finish().await?;
            shards.push(GcQuarantineShard {
                shard,
                location: output.to_string(),
                checksum: encode_digest(result.checksum),
                candidate_count: result.count,
                candidate_bytes: result.bytes,
            });
        }
        self.verify_shards(observation.id, digest, &shards).await?;
        Ok(GcRevalidationBuild { shards, stats })
    }

    async fn read_identity(
        &self,
        path: &Path,
        segid: Segid,
    ) -> Result<InventoryRecord, GcInventoryError> {
        let result = self.object_store.get(path).await?;
        let mut record = InventoryRecord::from_meta(segid, &result.meta);
        let mut stream = result.into_stream();
        let mut hasher = Sha256::new();
        let mut bytes = 0u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            bytes = checked_add(bytes, chunk.len() as u64, "candidate content bytes")?;
            hasher.update(&chunk);
        }
        if bytes != record.size {
            return Err(GcInventoryError::Corrupt(format!(
                "candidate {segid:?} body length disagrees with object metadata"
            )));
        }
        record = record.with_content_digest(hasher.finalize().into());
        Ok(record)
    }

    pub(crate) async fn verify_revalidation(
        &self,
        observation: &GcRevalidationRecord,
    ) -> Result<(), GcInventoryError> {
        self.verify_shards(
            observation.id,
            decode_digest(&observation.root_digest)?,
            &observation.candidate_shards,
        )
        .await
    }

    pub(crate) async fn delete_batch(
        &self,
        segment_pool: &str,
        observation: &GcRevalidationRecord,
        mut next_shard: u16,
        mut next_record: u64,
        limit: u32,
    ) -> Result<GcDeleteBatch, GcInventoryError> {
        if next_shard > 256 || limit == 0 {
            return Err(GcInventoryError::Corrupt(
                "invalid GC deletion cursor or batch limit".to_string(),
            ));
        }
        let digest = decode_digest(&observation.root_digest)?;
        let mut deleted_objects = 0u64;
        let mut deleted_bytes = 0u64;
        let mut already_absent = 0u64;
        let mut processed = 0u32;
        while next_shard < 256 && processed < limit {
            let descriptor = &observation.candidate_shards[next_shard as usize];
            if next_record > descriptor.candidate_count {
                return Err(GcInventoryError::Corrupt(
                    "GC deletion cursor exceeds its candidate shard".to_string(),
                ));
            }
            if next_record == descriptor.candidate_count {
                next_shard += 1;
                next_record = 0;
                continue;
            }
            let mut reader = InventoryReader::open_with_magic(
                Arc::clone(&self.object_store),
                &Path::from(descriptor.location.clone()),
                QUARANTINE_MAGIC,
                observation.id,
                digest,
                descriptor.shard,
            )
            .await?;
            let remaining = (limit - processed) as usize;
            let mut selected = Vec::with_capacity(remaining);
            let mut index = 0u64;
            while let Some(candidate) = reader.next().await? {
                if index >= next_record && selected.len() < remaining {
                    selected.push(candidate);
                }
                index = checked_add(index, 1, "candidate artifact record count")?;
            }
            let verified = reader.finish().await?;
            if verified.count != descriptor.candidate_count
                || verified.bytes != descriptor.candidate_bytes
                || encode_digest(verified.checksum) != descriptor.checksum
            {
                return Err(GcInventoryError::Corrupt(format!(
                    "candidate shard {} changed before deletion",
                    descriptor.shard
                )));
            }
            if next_record > index || selected.is_empty() {
                return Err(GcInventoryError::Corrupt(
                    "GC deletion cursor cannot select its next candidate".to_string(),
                ));
            }
            // Delete only records collected from the same fully checksummed
            // stream snapshot; never re-read an artifact after verification.
            for candidate in selected {
                let path = Path::from(format!("{segment_pool}/{}", candidate.segid.object_key()));
                match self.read_identity(&path, candidate.segid).await {
                    Ok(identity) => {
                        if identity != candidate {
                            return Err(GcInventoryError::Corrupt(format!(
                                "candidate {:?} changed before physical deletion",
                                candidate.segid
                            )));
                        }
                        self.object_store.delete(&path).await?;
                        match self.object_store.head(&path).await {
                            Err(object_store::Error::NotFound { .. }) => {}
                            Ok(_) => {
                                return Err(GcInventoryError::Corrupt(format!(
                                    "candidate {:?} remained after delete",
                                    candidate.segid
                                )));
                            }
                            Err(error) => return Err(error.into()),
                        }
                        deleted_objects =
                            checked_add(deleted_objects, 1, "physically deleted candidate count")?;
                        deleted_bytes = checked_add(
                            deleted_bytes,
                            candidate.size,
                            "physically deleted candidate bytes",
                        )?;
                    }
                    Err(GcInventoryError::ObjectStore(object_store::Error::NotFound {
                        ..
                    })) => {
                        already_absent = checked_add(
                            already_absent,
                            1,
                            "already absent deletion candidate count",
                        )?;
                    }
                    Err(error) => return Err(error),
                }
                processed += 1;
                next_record += 1;
            }
            if next_record == descriptor.candidate_count {
                next_shard += 1;
                next_record = 0;
            }
        }
        while next_shard < 256
            && observation.candidate_shards[next_shard as usize].candidate_count == 0
        {
            next_shard += 1;
        }
        Ok(GcDeleteBatch {
            next_shard,
            next_record,
            deleted_objects,
            deleted_bytes,
            already_absent,
        })
    }

    async fn verify_shards(
        &self,
        run_id: Uuid,
        digest: [u8; 32],
        shards: &[GcQuarantineShard],
    ) -> Result<(), GcInventoryError> {
        if shards.len() != 256 {
            return Err(GcInventoryError::Corrupt(
                "authoritative quarantine set is incomplete".to_string(),
            ));
        }
        for descriptor in shards {
            let mut reader = InventoryReader::open_with_magic(
                Arc::clone(&self.object_store),
                &Path::from(descriptor.location.clone()),
                QUARANTINE_MAGIC,
                run_id,
                digest,
                descriptor.shard,
            )
            .await?;
            while reader.next().await?.is_some() {}
            let result = reader.finish().await?;
            if result.count != descriptor.candidate_count
                || result.bytes != descriptor.candidate_bytes
                || encode_digest(result.checksum) != descriptor.checksum
            {
                return Err(GcInventoryError::Corrupt(format!(
                    "quarantine shard {} disagrees with its descriptor",
                    descriptor.shard
                )));
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct OnlineRuns {
    levels: Vec<Option<Path>>,
}

impl OnlineRuns {
    fn add_or_pair(&mut self, level: usize, carry: Path) -> Option<(Path, Path)> {
        if self.levels.len() <= level {
            self.levels.resize_with(level + 1, || None);
        }
        match self.levels[level].take() {
            Some(existing) => Some((existing, carry)),
            None => {
                self.levels[level] = Some(carry);
                None
            }
        }
    }

    fn into_paths(self) -> Vec<Path> {
        self.levels.into_iter().flatten().collect()
    }
}

#[derive(Clone, Copy)]
struct InventoryResult {
    count: u64,
    bytes: u64,
    checksum: [u8; 32],
}

struct InventoryWriter {
    writer: BufWriter,
    hasher: Sha256,
    count: u64,
    bytes: u64,
    last: Option<Segid>,
}

impl InventoryWriter {
    async fn new(
        store: Arc<dyn ObjectStore>,
        path: Path,
        magic: &[u8; 8],
        run_id: Uuid,
        digest: [u8; 32],
        shard: u8,
    ) -> Result<Self, GcInventoryError> {
        let mut writer = BufWriter::with_capacity(store, path, IO_BUFFER_BYTES);
        let header = file_header(magic, run_id, digest, shard);
        writer.write_all(&header).await?;
        let mut hasher = Sha256::new();
        hasher.update(header);
        Ok(Self {
            writer,
            hasher,
            count: 0,
            bytes: 0,
            last: None,
        })
    }

    async fn push(&mut self, record: InventoryRecord) -> Result<(), GcInventoryError> {
        if self.last.is_some_and(|last| last >= record.segid) {
            return Err(GcInventoryError::Corrupt(
                "inventory output is not strictly sorted and unique".to_string(),
            ));
        }
        let encoded = encode_record(record);
        self.writer.write_all(&encoded).await?;
        self.hasher.update(encoded);
        self.count = checked_add(self.count, 1, "inventory file record count")?;
        self.bytes = checked_add(self.bytes, record.size, "inventory file byte count")?;
        self.last = Some(record.segid);
        Ok(())
    }

    async fn finish(mut self) -> Result<InventoryResult, GcInventoryError> {
        let count = self.count.to_le_bytes();
        let bytes = self.bytes.to_le_bytes();
        self.writer.write_all(&count).await?;
        self.writer.write_all(&bytes).await?;
        self.hasher.update(count);
        self.hasher.update(bytes);
        let checksum: [u8; 32] = self.hasher.finalize().into();
        self.writer.write_all(&checksum).await?;
        self.writer.shutdown().await?;
        Ok(InventoryResult {
            count: self.count,
            bytes: self.bytes,
            checksum,
        })
    }
}

struct InventoryReader {
    reader: BufReader,
    hasher: Sha256,
    remaining: u64,
    expected_count: u64,
    last: Option<Segid>,
}

impl InventoryReader {
    async fn open(
        store: Arc<dyn ObjectStore>,
        path: &Path,
        run_id: Uuid,
        digest: [u8; 32],
        shard: u8,
    ) -> Result<Self, GcInventoryError> {
        Self::open_with_magic(store, path, INVENTORY_MAGIC, run_id, digest, shard).await
    }

    async fn open_with_magic(
        store: Arc<dyn ObjectStore>,
        path: &Path,
        magic: &[u8; 8],
        run_id: Uuid,
        digest: [u8; 32],
        shard: u8,
    ) -> Result<Self, GcInventoryError> {
        let meta = store.head(path).await?;
        let payload = meta
            .size
            .checked_sub((HEADER_LEN + FOOTER_LEN) as u64)
            .ok_or_else(|| {
                GcInventoryError::Corrupt(format!("inventory file {path} is truncated"))
            })?;
        if payload % RECORD_LEN as u64 != 0 {
            return Err(GcInventoryError::Corrupt(format!(
                "inventory file {path} has a partial record"
            )));
        }
        let expected_count = payload / RECORD_LEN as u64;
        let mut reader = BufReader::with_capacity(store, &meta, IO_BUFFER_BYTES);
        let mut header = [0u8; HEADER_LEN];
        reader.read_exact(&mut header).await?;
        if header != file_header(magic, run_id, digest, shard) {
            return Err(GcInventoryError::Corrupt(format!(
                "inventory file {path} has the wrong identity"
            )));
        }
        let mut hasher = Sha256::new();
        hasher.update(header);
        Ok(Self {
            reader,
            hasher,
            remaining: expected_count,
            expected_count,
            last: None,
        })
    }

    async fn next(&mut self) -> Result<Option<InventoryRecord>, GcInventoryError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut encoded = [0u8; RECORD_LEN];
        self.reader.read_exact(&mut encoded).await?;
        self.hasher.update(encoded);
        let record = decode_record(encoded)?;
        if self.last.is_some_and(|last| last >= record.segid) {
            return Err(GcInventoryError::Corrupt(
                "inventory file is not strictly sorted and unique".to_string(),
            ));
        }
        self.last = Some(record.segid);
        self.remaining -= 1;
        Ok(Some(record))
    }

    async fn finish(mut self) -> Result<InventoryResult, GcInventoryError> {
        if self.remaining != 0 {
            return Err(GcInventoryError::Corrupt(
                "inventory reader finished before all records".to_string(),
            ));
        }
        let mut count = [0u8; 8];
        let mut bytes = [0u8; 8];
        self.reader.read_exact(&mut count).await?;
        self.reader.read_exact(&mut bytes).await?;
        self.hasher.update(count);
        self.hasher.update(bytes);
        let count = u64::from_le_bytes(count);
        let bytes = u64::from_le_bytes(bytes);
        if count != self.expected_count {
            return Err(GcInventoryError::Corrupt(
                "inventory footer count is inconsistent".to_string(),
            ));
        }
        let mut checksum = [0u8; 32];
        self.reader.read_exact(&mut checksum).await?;
        let actual: [u8; 32] = self.hasher.finalize().into();
        if checksum != actual {
            return Err(GcInventoryError::Corrupt(
                "inventory checksum mismatch".to_string(),
            ));
        }
        let mut extra = [0u8; 1];
        if self.reader.read(&mut extra).await? != 0 {
            return Err(GcInventoryError::Corrupt(
                "inventory file has trailing bytes".to_string(),
            ));
        }
        Ok(InventoryResult {
            count,
            bytes,
            checksum,
        })
    }
}

async fn write_inventory_file(
    store: Arc<dyn ObjectStore>,
    path: &Path,
    run_id: Uuid,
    digest: [u8; 32],
    shard: u8,
    records: &[InventoryRecord],
) -> Result<InventoryResult, GcInventoryError> {
    let mut writer =
        InventoryWriter::new(store, path.clone(), INVENTORY_MAGIC, run_id, digest, shard).await?;
    for record in records {
        writer.push(*record).await?;
    }
    writer.finish().await
}

async fn merge_inventory_files(
    store: Arc<dyn ObjectStore>,
    run_id: Uuid,
    digest: [u8; 32],
    shard: u8,
    inputs: &[Path],
    output: &Path,
) -> Result<InventoryResult, GcInventoryError> {
    if inputs.len() > 2 {
        return Err(GcInventoryError::Corrupt(
            "bounded inventory merge accepts at most two inputs".to_string(),
        ));
    }
    let mut readers = Vec::with_capacity(inputs.len());
    for path in inputs {
        readers.push(InventoryReader::open(Arc::clone(&store), path, run_id, digest, shard).await?);
    }
    let mut heads = Vec::with_capacity(readers.len());
    for reader in &mut readers {
        heads.push(reader.next().await?);
    }
    let mut writer = InventoryWriter::new(
        Arc::clone(&store),
        output.clone(),
        INVENTORY_MAGIC,
        run_id,
        digest,
        shard,
    )
    .await?;
    loop {
        let next = heads
            .iter()
            .flatten()
            .min_by_key(|record| record.segid)
            .copied();
        let Some(next) = next else { break };
        writer.push(next).await?;
        for (head, reader) in heads.iter_mut().zip(&mut readers) {
            if head
                .as_ref()
                .is_some_and(|record| record.segid == next.segid)
            {
                if *head != Some(next) {
                    return Err(GcInventoryError::Corrupt(
                        "duplicate physical segment has conflicting metadata".to_string(),
                    ));
                }
                *head = reader.next().await?;
            }
        }
    }
    for reader in readers {
        reader.finish().await?;
    }
    writer.finish().await
}

fn file_header(magic: &[u8; 8], run_id: Uuid, digest: [u8; 32], shard: u8) -> [u8; HEADER_LEN] {
    let mut header = [0u8; HEADER_LEN];
    header[..8].copy_from_slice(magic);
    header[8..12].copy_from_slice(&FILE_VERSION.to_le_bytes());
    header[12..28].copy_from_slice(run_id.as_bytes());
    header[28..60].copy_from_slice(&digest);
    header[60] = shard;
    header
}

fn encode_record(record: InventoryRecord) -> [u8; RECORD_LEN] {
    let mut encoded = [0u8; RECORD_LEN];
    encoded[..8].copy_from_slice(&record.segid.epoch.to_be_bytes());
    encoded[8..16].copy_from_slice(&record.segid.counter.to_be_bytes());
    encoded[16..24].copy_from_slice(&record.size.to_le_bytes());
    encoded[24..32].copy_from_slice(&record.modified_seconds.to_le_bytes());
    encoded[32..36].copy_from_slice(&record.modified_nanos.to_le_bytes());
    encoded[36..68].copy_from_slice(&record.content_digest);
    encoded
}

fn decode_record(encoded: [u8; RECORD_LEN]) -> Result<InventoryRecord, GcInventoryError> {
    let record = InventoryRecord {
        segid: Segid::new(
            u64::from_be_bytes(encoded[..8].try_into().expect("fixed slice")),
            u64::from_be_bytes(encoded[8..16].try_into().expect("fixed slice")),
        ),
        size: u64::from_le_bytes(encoded[16..24].try_into().expect("fixed slice")),
        modified_seconds: i64::from_le_bytes(encoded[24..32].try_into().expect("fixed slice")),
        modified_nanos: u32::from_le_bytes(encoded[32..36].try_into().expect("fixed slice")),
        content_digest: encoded[36..68].try_into().expect("fixed slice"),
    };
    record.modified_at()?;
    Ok(record)
}

fn parse_segment_path(
    pool: &str,
    expected_shard: u8,
    path: &Path,
) -> Result<Segid, GcInventoryError> {
    let full = path.as_ref();
    let relative = full
        .strip_prefix(pool)
        .and_then(|value| value.strip_prefix('/'))
        .ok_or_else(|| {
            GcInventoryError::Corrupt(format!(
                "inventory object {path} escapes segment pool {pool}"
            ))
        })?;
    let segid = Segid::from_object_key(relative).ok_or_else(|| {
        GcInventoryError::Corrupt(format!("inventory object {path} is not a segment key"))
    })?;
    if segid.counter & 0xff != u64::from(expected_shard) || segid.object_key() != relative {
        return Err(GcInventoryError::Corrupt(format!(
            "inventory object {path} has a noncanonical shard or segment key"
        )));
    }
    Ok(segid)
}

fn segment_prefix(pool: &str, shard: u8) -> Path {
    Path::from(format!("{pool}/segments/{shard:02x}"))
}

fn inventory_run_path(run_id: Uuid, shard: u8, sequence: u64) -> Path {
    Path::from(format!(
        "__zerofs_gc/{run_id}/inventory-runs/{shard:02x}/{sequence:020}.bin"
    ))
}

fn inventory_online_path(run_id: Uuid, shard: u8, level: usize, sequence: u64) -> Path {
    Path::from(format!(
        "__zerofs_gc/{run_id}/inventory-online/{shard:02x}/{level:08}/{sequence:020}.bin"
    ))
}

fn inventory_final_path(run_id: Uuid, shard: u8) -> Path {
    Path::from(format!("__zerofs_gc/{run_id}/inventory/{shard:02x}.bin"))
}

fn revalidation_path(run_id: Uuid, observation_id: Uuid, shard: u8) -> Path {
    Path::from(format!(
        "__zerofs_gc/{run_id}/revalidation/{observation_id}/{shard:02x}.bin"
    ))
}

fn quarantine_path(run_id: Uuid, shard: u8) -> Path {
    Path::from(format!("__zerofs_gc/{run_id}/quarantine/{shard:02x}.bin"))
}

fn checked_add(left: u64, right: u64, label: &str) -> Result<u64, GcInventoryError> {
    left.checked_add(right)
        .ok_or_else(|| GcInventoryError::Corrupt(format!("{label} overflow")))
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum GcInventoryError {
    #[error("corrupt GC inventory data: {0}")]
    Corrupt(String),
    #[error(transparent)]
    Mark(#[from] GcMarkError),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions,
        PutOptions, PutPayload, PutResult,
    };

    #[derive(Debug)]
    struct FixedTimestampStore {
        inner: Arc<dyn ObjectStore>,
        timestamp: DateTime<Utc>,
    }

    impl std::fmt::Display for FixedTimestampStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "fixed-timestamp")
        }
    }

    #[async_trait]
    impl ObjectStore for FixedTimestampStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let mut result = self.inner.get_opts(location, options).await?;
            result.meta.last_modified = self.timestamp;
            Ok(result)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            let timestamp = self.timestamp;
            self.inner
                .list(prefix)
                .map(move |result| {
                    result.map(|mut meta| {
                        meta.last_modified = timestamp;
                        meta
                    })
                })
                .boxed()
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            let mut result = self.inner.list_with_delimiter(prefix).await?;
            for meta in &mut result.objects {
                meta.last_modified = self.timestamp;
            }
            Ok(result)
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn record(counter: u64) -> InventoryRecord {
        InventoryRecord {
            segid: Segid::new(11, counter),
            size: counter + 1,
            modified_seconds: 1_700_000_000,
            modified_nanos: 0,
            content_digest: [0; 32],
        }
    }

    #[test]
    fn same_size_same_timestamp_replacement_has_a_different_identity() {
        let original = record(1).with_content_digest(Sha256::digest(b"original").into());
        let replacement = record(1).with_content_digest(Sha256::digest(b"replaced").into());
        assert!(original.metadata_matches(replacement));
        assert_ne!(original, replacement);
        assert_ne!(encode_record(original), encode_record(replacement));
    }

    #[tokio::test]
    async fn streamed_identity_rejects_same_size_same_timestamp_changed_content() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let timestamp = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(FixedTimestampStore { inner, timestamp });
        let path = Path::from("candidate");
        store
            .put(&path, bytes::Bytes::from_static(b"original").into())
            .await
            .unwrap();
        let inventory = GcInventoryStore {
            object_store: Arc::clone(&store),
        };
        let segid = Segid::new(1, 1);
        let original = inventory.read_identity(&path, segid).await.unwrap();
        store
            .put(&path, bytes::Bytes::from_static(b"replaced").into())
            .await
            .unwrap();
        let replacement = inventory.read_identity(&path, segid).await.unwrap();
        assert!(original.metadata_matches(replacement));
        assert_ne!(original.content_digest, replacement.content_digest);
        assert_ne!(original, replacement);
    }

    #[tokio::test]
    async fn deletion_batch_is_bounded_and_replay_treats_prior_delete_as_absent() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let timestamp = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(FixedTimestampStore { inner, timestamp });
        let pool = "pool";
        let segid = Segid::new(4, 0);
        let object = Path::from(format!("{pool}/{}", segid.object_key()));
        store
            .put(&object, bytes::Bytes::from_static(b"candidate").into())
            .await
            .unwrap();
        let inventory = GcInventoryStore {
            object_store: Arc::clone(&store),
        };
        let candidate = inventory.read_identity(&object, segid).await.unwrap();
        let observation_id = Uuid::new_v4();
        let digest = [7u8; 32];
        let artifact = Path::from("candidate-shard");
        let mut writer = InventoryWriter::new(
            Arc::clone(&store),
            artifact.clone(),
            QUARANTINE_MAGIC,
            observation_id,
            digest,
            0,
        )
        .await
        .unwrap();
        writer.push(candidate).await.unwrap();
        let result = writer.finish().await.unwrap();
        let mut shards = (0u8..=u8::MAX)
            .map(|shard| GcQuarantineShard {
                shard,
                location: format!("unused-{shard}"),
                checksum: "00".repeat(32),
                candidate_count: 0,
                candidate_bytes: 0,
            })
            .collect::<Vec<_>>();
        shards[0] = GcQuarantineShard {
            shard: 0,
            location: artifact.to_string(),
            checksum: encode_digest(result.checksum),
            candidate_count: 1,
            candidate_bytes: candidate.size,
        };
        let observation = GcRevalidationRecord {
            id: observation_id,
            catalog_generation: 0,
            grace_seconds: 390,
            not_before: timestamp,
            inventory_cutoff: timestamp,
            roots: Vec::new(),
            root_digest: encode_digest(digest),
            mark_shards: Vec::new(),
            mark_stats: None,
            candidate_shards: shards,
            stats: None,
            captured_at: timestamp,
            completed_at: Some(timestamp),
        };
        let first = inventory
            .delete_batch(pool, &observation, 0, 0, 1)
            .await
            .unwrap();
        assert_eq!(first.next_shard, 256);
        assert_eq!(first.deleted_objects, 1);
        assert_eq!(first.already_absent, 0);
        let replay = inventory
            .delete_batch(pool, &observation, 0, 0, 1)
            .await
            .unwrap();
        assert_eq!(replay.next_shard, 256);
        assert_eq!(replay.deleted_objects, 0);
        assert_eq!(replay.already_absent, 1);
    }

    #[test]
    fn inventory_online_runs_keep_only_logarithmically_many_paths() {
        let mut runs = OnlineRuns::default();
        for sequence in 0..1_000_000u64 {
            let mut carry = Path::from(format!("run-{sequence}"));
            let mut level = 0usize;
            loop {
                let Some(_) = runs.add_or_pair(level, carry) else {
                    break;
                };
                carry = Path::from(format!("merge-{level}-{sequence}"));
                level += 1;
            }
            let flushes = sequence + 1;
            let resident = runs.levels.iter().flatten().count();
            let logarithmic_bound = (u64::BITS - flushes.leading_zeros()) as usize;
            assert!(resident <= logarithmic_bound);
            assert_eq!(resident, flushes.count_ones() as usize);
        }
    }

    #[tokio::test]
    async fn merge_orders_interleaved_unordered_listing_runs() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let run_id = Uuid::new_v4();
        let digest = [7u8; 32];
        let left = Path::from("left");
        let right = Path::from("right");
        write_inventory_file(
            Arc::clone(&store),
            &left,
            run_id,
            digest,
            0,
            &[record(1), record(5), record(9)],
        )
        .await
        .unwrap();
        write_inventory_file(
            Arc::clone(&store),
            &right,
            run_id,
            digest,
            0,
            &[record(2), record(4), record(8)],
        )
        .await
        .unwrap();
        let output = Path::from("merged");
        merge_inventory_files(
            Arc::clone(&store),
            run_id,
            digest,
            0,
            &[left, right],
            &output,
        )
        .await
        .unwrap();
        let mut reader = InventoryReader::open(store, &output, run_id, digest, 0)
            .await
            .unwrap();
        let mut counters = Vec::new();
        while let Some(record) = reader.next().await.unwrap() {
            counters.push(record.segid.counter);
        }
        reader.finish().await.unwrap();
        assert_eq!(counters, vec![1, 2, 4, 5, 8, 9]);
    }
}
