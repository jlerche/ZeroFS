use super::{GcMarkShard, GcMarkStats, GcRunRecord, SlateDbRootStore};
use crate::fs::key_codec::{KeyCodec, KeyPrefix};
use crate::segment::{FrameLoc, Segid};
use object_store::buffered::{BufReader, BufWriter};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use sha2::{Digest, Sha256};
use std::array;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

const MARK_MAGIC: &[u8; 8] = b"ZFGCMK01";
const MARK_VERSION: u32 = 1;
const MARK_HEADER_LEN: usize = 8 + 4 + 16 + 32 + 1;
const MARK_RECORD_LEN: usize = 16;
const MARK_FOOTER_LEN: usize = 8 + 32;
const ENUMERATION_BUFFER_SEGMENTS: usize = 8_192;
const IO_BUFFER_BYTES: usize = 256 * 1024;

pub(crate) struct GcMarkStore {
    roots: SlateDbRootStore,
    object_store: Arc<dyn ObjectStore>,
}

pub(crate) struct GcMarkBuild {
    pub(crate) shards: Vec<GcMarkShard>,
    pub(crate) stats: GcMarkStats,
}

impl GcMarkStore {
    pub(crate) fn new(roots: SlateDbRootStore) -> Self {
        let object_store = roots.object_store();
        Self {
            roots,
            object_store,
        }
    }

    pub(crate) async fn build(&self, run: &GcRunRecord) -> Result<GcMarkBuild, GcMarkError> {
        let digest = decode_digest(&run.root_digest)?;
        let mut buffers: [Vec<Segid>; 256] = array::from_fn(|_| Vec::new());
        let mut run_paths: [OnlineRuns; 256] = array::from_fn(|_| OnlineRuns::default());
        let mut buffered = 0usize;
        let mut sequence = 0u64;
        let mut references_enumerated = 0u64;
        let mut intermediate_runs = 0u64;
        let codec = KeyCodec::new();
        let (start, end) = codec.prefix_range(KeyPrefix::Extent);

        for pin in &run.roots {
            let reader = self.roots.checkpoint_reader(&pin.root).await?;
            let mut entries = reader.scan(start.clone()..end.clone()).await?;
            while let Some(entry) = entries.next().await? {
                let location = FrameLoc::decode(&entry.value).ok_or_else(|| {
                    GcMarkError::Corrupt(format!(
                        "root {} contains a malformed extent value",
                        pin.root.identity
                    ))
                })?;
                let shard = (location.segid.counter & 0xff) as usize;
                buffers[shard].push(location.segid);
                references_enumerated = references_enumerated.checked_add(1).ok_or_else(|| {
                    GcMarkError::Corrupt("enumerated reference count overflow".to_string())
                })?;
                buffered += 1;
                if buffered >= ENUMERATION_BUFFER_SEGMENTS {
                    self.flush_buffers(
                        run.id,
                        digest,
                        sequence,
                        &mut buffers,
                        &mut run_paths,
                        &mut intermediate_runs,
                    )
                    .await?;
                    sequence = sequence.checked_add(1).ok_or_else(|| {
                        GcMarkError::Corrupt("mark-run sequence overflow".to_string())
                    })?;
                    buffered = 0;
                }
            }
            drop(entries);
            reader.close().await?;
        }
        if buffered != 0 {
            self.flush_buffers(
                run.id,
                digest,
                sequence,
                &mut buffers,
                &mut run_paths,
                &mut intermediate_runs,
            )
            .await?;
        }

        let mut shards = Vec::with_capacity(256);
        for shard in 0u8..=u8::MAX {
            shards.push(
                self.merge_shard(
                    run.id,
                    digest,
                    shard,
                    std::mem::take(&mut run_paths[shard as usize]).into_paths(),
                )
                .await?,
            );
        }
        self.verify_all(run.id, digest, &shards).await?;
        let unique_segments = shards.iter().try_fold(0u64, |total, shard| {
            total
                .checked_add(shard.segment_count)
                .ok_or_else(|| GcMarkError::Corrupt("unique segment count overflow".to_string()))
        })?;
        Ok(GcMarkBuild {
            shards,
            stats: GcMarkStats {
                roots_enumerated: run.roots.len() as u64,
                references_enumerated,
                intermediate_runs,
                unique_segments,
            },
        })
    }

    async fn flush_buffers(
        &self,
        run_id: Uuid,
        digest: [u8; 32],
        sequence: u64,
        buffers: &mut [Vec<Segid>; 256],
        paths: &mut [OnlineRuns; 256],
        intermediate_runs: &mut u64,
    ) -> Result<(), GcMarkError> {
        for shard in 0u8..=u8::MAX {
            let values = &mut buffers[shard as usize];
            if values.is_empty() {
                continue;
            }
            values.sort_unstable();
            values.dedup();
            let path = intermediate_path(run_id, shard, sequence);
            write_known_file(
                Arc::clone(&self.object_store),
                &path,
                run_id,
                digest,
                shard,
                values,
            )
            .await?;
            values.clear();
            *intermediate_runs = intermediate_runs.checked_add(1).ok_or_else(|| {
                GcMarkError::Corrupt("intermediate run count overflow".to_string())
            })?;
            self.insert_run(
                run_id,
                digest,
                shard,
                sequence,
                path,
                &mut paths[shard as usize],
            )
            .await?;
        }
        Ok(())
    }

    async fn insert_run(
        &self,
        run_id: Uuid,
        digest: [u8; 32],
        shard: u8,
        sequence: u64,
        mut carry: Path,
        runs: &mut OnlineRuns,
    ) -> Result<(), GcMarkError> {
        let mut level = 0usize;
        loop {
            let Some(existing) = runs.add_or_pair(level, carry) else {
                return Ok(());
            };
            let output = online_merge_path(run_id, shard, level, sequence);
            merge_files(
                Arc::clone(&self.object_store),
                run_id,
                digest,
                shard,
                &[existing.0, existing.1],
                &output,
            )
            .await?;
            carry = output;
            level = level
                .checked_add(1)
                .ok_or_else(|| GcMarkError::Corrupt("online merge level overflow".to_string()))?;
        }
    }

    async fn merge_shard(
        &self,
        run_id: Uuid,
        digest: [u8; 32],
        shard: u8,
        mut paths: Vec<Path>,
    ) -> Result<GcMarkShard, GcMarkError> {
        let mut round = 0u32;
        while paths.len() > 1 {
            let mut next = Vec::with_capacity(paths.len().div_ceil(2));
            for (pair, chunk) in paths.chunks(2).enumerate() {
                let output = merge_path(run_id, shard, round, pair as u64);
                merge_files(
                    Arc::clone(&self.object_store),
                    run_id,
                    digest,
                    shard,
                    chunk,
                    &output,
                )
                .await?;
                next.push(output);
            }
            paths = next;
            round = round
                .checked_add(1)
                .ok_or_else(|| GcMarkError::Corrupt("mark merge round overflow".to_string()))?;
        }
        let final_path = final_path(run_id, shard);
        let result = merge_files(
            Arc::clone(&self.object_store),
            run_id,
            digest,
            shard,
            &paths,
            &final_path,
        )
        .await?;
        Ok(GcMarkShard {
            shard,
            location: final_path.to_string(),
            checksum: encode_digest(result.checksum),
            segment_count: result.count,
        })
    }

    pub(crate) async fn verify_all(
        &self,
        run_id: Uuid,
        digest: [u8; 32],
        shards: &[GcMarkShard],
    ) -> Result<(), GcMarkError> {
        if shards.len() != 256 {
            return Err(GcMarkError::Corrupt(
                "authoritative mark set is incomplete".to_string(),
            ));
        }
        for descriptor in shards {
            let path = Path::from(descriptor.location.clone());
            let mut reader = MarkReader::open(
                Arc::clone(&self.object_store),
                &path,
                run_id,
                digest,
                descriptor.shard,
            )
            .await?;
            while reader.next().await?.is_some() {}
            let result = reader.finish().await?;
            if result.count != descriptor.segment_count
                || encode_digest(result.checksum) != descriptor.checksum
            {
                return Err(GcMarkError::Corrupt(format!(
                    "mark shard {} disagrees with its descriptor",
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
    /// Store a run at `level`, or return it paired with the existing run so
    /// the caller can merge the pair and carry the result to the next level.
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

    #[cfg(test)]
    fn resident_paths(&self) -> usize {
        self.levels.iter().flatten().count()
    }
}

#[derive(Clone, Copy)]
struct MarkResult {
    count: u64,
    checksum: [u8; 32],
}

struct MarkWriter {
    writer: BufWriter,
    hasher: Sha256,
    count: u64,
    last: Option<Segid>,
}

impl MarkWriter {
    async fn new(
        store: Arc<dyn ObjectStore>,
        path: Path,
        run_id: Uuid,
        digest: [u8; 32],
        shard: u8,
    ) -> Result<Self, GcMarkError> {
        let mut writer = BufWriter::with_capacity(store, path, IO_BUFFER_BYTES);
        let header = mark_header(run_id, digest, shard);
        writer.write_all(&header).await?;
        let mut hasher = Sha256::new();
        hasher.update(header);
        Ok(Self {
            writer,
            hasher,
            count: 0,
            last: None,
        })
    }

    async fn push(&mut self, value: Segid) -> Result<(), GcMarkError> {
        if self.last == Some(value) {
            return Ok(());
        }
        if self.last.is_some_and(|last| last > value) {
            return Err(GcMarkError::Corrupt(
                "mark merge produced unsorted output".to_string(),
            ));
        }
        let bytes = encode_segid(value);
        self.writer.write_all(&bytes).await?;
        self.hasher.update(bytes);
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| GcMarkError::Corrupt("mark count overflow".to_string()))?;
        self.last = Some(value);
        Ok(())
    }

    async fn finish(mut self) -> Result<MarkResult, GcMarkError> {
        let count = self.count.to_le_bytes();
        self.writer.write_all(&count).await?;
        self.hasher.update(count);
        let checksum: [u8; 32] = self.hasher.finalize().into();
        self.writer.write_all(&checksum).await?;
        self.writer.shutdown().await?;
        Ok(MarkResult {
            count: self.count,
            checksum,
        })
    }
}

struct MarkReader {
    reader: BufReader,
    hasher: Sha256,
    remaining: u64,
    expected_count: u64,
    last: Option<Segid>,
}

impl MarkReader {
    async fn open(
        store: Arc<dyn ObjectStore>,
        path: &Path,
        run_id: Uuid,
        digest: [u8; 32],
        shard: u8,
    ) -> Result<Self, GcMarkError> {
        let meta = store.head(path).await?;
        let payload = meta
            .size
            .checked_sub((MARK_HEADER_LEN + MARK_FOOTER_LEN) as u64)
            .ok_or_else(|| GcMarkError::Corrupt(format!("mark file {path} is truncated")))?;
        if payload % MARK_RECORD_LEN as u64 != 0 {
            return Err(GcMarkError::Corrupt(format!(
                "mark file {path} has a partial record"
            )));
        }
        let expected_count = payload / MARK_RECORD_LEN as u64;
        let mut reader = BufReader::with_capacity(store, &meta, IO_BUFFER_BYTES);
        let mut header = [0u8; MARK_HEADER_LEN];
        reader.read_exact(&mut header).await?;
        if header != mark_header(run_id, digest, shard) {
            return Err(GcMarkError::Corrupt(format!(
                "mark file {path} has the wrong identity"
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

    async fn next(&mut self) -> Result<Option<Segid>, GcMarkError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut bytes = [0u8; MARK_RECORD_LEN];
        self.reader.read_exact(&mut bytes).await?;
        self.hasher.update(bytes);
        let value = decode_segid(bytes);
        if self.last.is_some_and(|last| last >= value) {
            return Err(GcMarkError::Corrupt(
                "mark file is not strictly sorted and deduplicated".to_string(),
            ));
        }
        self.last = Some(value);
        self.remaining -= 1;
        Ok(Some(value))
    }

    async fn finish(mut self) -> Result<MarkResult, GcMarkError> {
        if self.remaining != 0 {
            return Err(GcMarkError::Corrupt(
                "mark reader finished before consuming every record".to_string(),
            ));
        }
        let mut count = [0u8; 8];
        self.reader.read_exact(&mut count).await?;
        self.hasher.update(count);
        let count = u64::from_le_bytes(count);
        if count != self.expected_count {
            return Err(GcMarkError::Corrupt(
                "mark footer count is inconsistent".to_string(),
            ));
        }
        let mut checksum = [0u8; 32];
        self.reader.read_exact(&mut checksum).await?;
        let actual: [u8; 32] = self.hasher.finalize().into();
        if checksum != actual {
            return Err(GcMarkError::Corrupt("mark checksum mismatch".to_string()));
        }
        let mut extra = [0u8; 1];
        if self.reader.read(&mut extra).await? != 0 {
            return Err(GcMarkError::Corrupt(
                "mark file has trailing bytes".to_string(),
            ));
        }
        Ok(MarkResult { count, checksum })
    }
}

async fn merge_files(
    store: Arc<dyn ObjectStore>,
    run_id: Uuid,
    digest: [u8; 32],
    shard: u8,
    inputs: &[Path],
    output: &Path,
) -> Result<MarkResult, GcMarkError> {
    if inputs.len() > 2 {
        return Err(GcMarkError::Corrupt(
            "bounded mark merge accepts at most two inputs".to_string(),
        ));
    }
    let mut readers = Vec::with_capacity(inputs.len());
    for path in inputs {
        readers.push(MarkReader::open(Arc::clone(&store), path, run_id, digest, shard).await?);
    }
    let mut heads = Vec::with_capacity(readers.len());
    for reader in &mut readers {
        heads.push(reader.next().await?);
    }
    let mut writer =
        MarkWriter::new(Arc::clone(&store), output.clone(), run_id, digest, shard).await?;
    loop {
        let next = heads.iter().flatten().copied().min();
        let Some(next) = next else { break };
        writer.push(next).await?;
        for (head, reader) in heads.iter_mut().zip(&mut readers) {
            if *head == Some(next) {
                *head = reader.next().await?;
            }
        }
    }
    for reader in readers {
        reader.finish().await?;
    }
    writer.finish().await
}

async fn write_known_file(
    store: Arc<dyn ObjectStore>,
    path: &Path,
    run_id: Uuid,
    digest: [u8; 32],
    shard: u8,
    values: &[Segid],
) -> Result<MarkResult, GcMarkError> {
    let mut writer = MarkWriter::new(store, path.clone(), run_id, digest, shard).await?;
    for value in values {
        writer.push(*value).await?;
    }
    writer.finish().await
}

fn mark_header(run_id: Uuid, digest: [u8; 32], shard: u8) -> [u8; MARK_HEADER_LEN] {
    let mut header = [0u8; MARK_HEADER_LEN];
    header[..8].copy_from_slice(MARK_MAGIC);
    header[8..12].copy_from_slice(&MARK_VERSION.to_le_bytes());
    header[12..28].copy_from_slice(run_id.as_bytes());
    header[28..60].copy_from_slice(&digest);
    header[60] = shard;
    header
}

fn encode_segid(value: Segid) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&value.epoch.to_be_bytes());
    bytes[8..].copy_from_slice(&value.counter.to_be_bytes());
    bytes
}

fn decode_segid(bytes: [u8; 16]) -> Segid {
    Segid::new(
        u64::from_be_bytes(bytes[..8].try_into().expect("fixed slice")),
        u64::from_be_bytes(bytes[8..].try_into().expect("fixed slice")),
    )
}

fn decode_digest(value: &str) -> Result<[u8; 32], GcMarkError> {
    if value.len() != 64 {
        return Err(GcMarkError::Corrupt("invalid root digest".to_string()));
    }
    let mut output = [0u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| GcMarkError::Corrupt("invalid root digest".to_string()))?;
    }
    Ok(output)
}

fn encode_digest(value: [u8; 32]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn intermediate_path(run_id: Uuid, shard: u8, sequence: u64) -> Path {
    Path::from(format!(
        "__zerofs_gc/{run_id}/mark-runs/{shard:02x}/{sequence:020}.bin"
    ))
}

fn merge_path(run_id: Uuid, shard: u8, round: u32, pair: u64) -> Path {
    Path::from(format!(
        "__zerofs_gc/{run_id}/mark-merge/{shard:02x}/{round:08}/{pair:020}.bin"
    ))
}

fn online_merge_path(run_id: Uuid, shard: u8, level: usize, sequence: u64) -> Path {
    Path::from(format!(
        "__zerofs_gc/{run_id}/mark-online/{shard:02x}/{level:08}/{sequence:020}.bin"
    ))
}

fn final_path(run_id: Uuid, shard: u8) -> Path {
    Path::from(format!("__zerofs_gc/{run_id}/marks/{shard:02x}.bin"))
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum GcMarkError {
    #[error("corrupt GC mark data: {0}")]
    Corrupt(String),
    #[error(transparent)]
    RootStore(#[from] super::RootStoreError),
    #[error(transparent)]
    SlateDb(#[from] slatedb::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn online_runs_keep_only_logarithmically_many_path_handles() {
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
            let logarithmic_bound = (u64::BITS - flushes.leading_zeros()) as usize;
            assert!(runs.resident_paths() <= logarithmic_bound);
            assert_eq!(runs.resident_paths(), flushes.count_ones() as usize);
        }
    }
}
