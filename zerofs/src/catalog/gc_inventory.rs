use super::gc_mark::{GcMarkError, MarkReader, decode_digest, encode_digest};
use super::{GcInventoryStats, GcQuarantineShard, GcRunRecord, SlateDbRootStore};
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
const FILE_VERSION: u32 = 1;
const HEADER_LEN: usize = 8 + 4 + 16 + 32 + 1;
const RECORD_LEN: usize = 16 + 8 + 8 + 4;
const FOOTER_LEN: usize = 8 + 8 + 32;
const INVENTORY_BUFFER_OBJECTS: usize = 8_192;
const IO_BUFFER_BYTES: usize = 256 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct InventoryRecord {
    segid: Segid,
    size: u64,
    modified_seconds: i64,
    modified_nanos: u32,
}

impl InventoryRecord {
    fn from_meta(segid: Segid, meta: &ObjectMeta) -> Self {
        Self {
            segid,
            size: meta.size,
            modified_seconds: meta.last_modified.timestamp(),
            modified_nanos: meta.last_modified.timestamp_subsec_nanos(),
        }
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
                writer.push(record).await?;
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
                run.id,
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
    use object_store::memory::InMemory;

    fn record(counter: u64) -> InventoryRecord {
        InventoryRecord {
            segid: Segid::new(11, counter),
            size: counter + 1,
            modified_seconds: 1_700_000_000,
            modified_nanos: 0,
        }
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
