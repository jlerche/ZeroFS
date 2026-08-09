use super::{GcMarkShard, GcMarkStats, GcRootPin, GcRunRecord, SlateDbRootStore};
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
#[cfg(test)]
pub(crate) const MARK_FILE_OVERHEAD_BYTES: u64 = (MARK_HEADER_LEN + MARK_FOOTER_LEN) as u64;
#[cfg(test)]
pub(crate) const MARK_RECORD_BYTES: u64 = MARK_RECORD_LEN as u64;

/// Maximum number of times any record is written after `initial_runs` sorted
/// runs enter the binary-carry pipeline: once for its initial run, once per
/// possible carry level, once per bounded finalization round, and once for the
/// authoritative final file.
pub(crate) fn binary_carry_max_write_passes(initial_runs: u64) -> u32 {
    if initial_runs == 0 {
        return 0;
    }
    let carry_levels = u64::BITS - 1 - initial_runs.leading_zeros();
    let resident_runs = initial_runs.count_ones();
    let final_rounds = if resident_runs <= 1 {
        0
    } else {
        u32::BITS - (resident_runs - 1).leading_zeros()
    };
    2 + carry_levels + final_rounds
}

/// Monotone validation envelope when only the total runs across all shards is
/// available. For every `r <= total_runs`, the carry depth is at most the
/// global floor and `popcount(r)` is at most that floor plus one.
pub(crate) fn binary_carry_global_write_pass_upper_bound(total_runs: u64) -> u32 {
    if total_runs == 0 {
        return 0;
    }
    let global_floor = u64::BITS - 1 - total_runs.leading_zeros();
    let max_popcount = global_floor + 1;
    let final_rounds = if max_popcount <= 1 {
        0
    } else {
        u32::BITS - (max_popcount - 1).leading_zeros()
    };
    2 + global_floor + final_rounds
}

pub(crate) struct GcMarkStore {
    roots: SlateDbRootStore,
    object_store: Arc<dyn ObjectStore>,
}

pub(crate) struct GcMarkBuild {
    pub(crate) shards: Vec<GcMarkShard>,
    pub(crate) stats: GcMarkStats,
}

struct MarkBuildState {
    buffers: [Vec<Segid>; 256],
    run_paths: [OnlineRuns; 256],
    buffered: usize,
    sequence: u64,
    references_enumerated: u64,
    intermediate_runs: u64,
}

impl Default for MarkBuildState {
    fn default() -> Self {
        Self {
            buffers: array::from_fn(|_| Vec::new()),
            run_paths: array::from_fn(|_| OnlineRuns::default()),
            buffered: 0,
            sequence: 0,
            references_enumerated: 0,
            intermediate_runs: 0,
        }
    }
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
        self.build_observation(run.id, &run.root_digest, &run.roots)
            .await
    }

    pub(crate) async fn build_observation(
        &self,
        run_id: Uuid,
        root_digest: &str,
        roots: &[GcRootPin],
    ) -> Result<GcMarkBuild, GcMarkError> {
        let digest = decode_digest(root_digest)?;
        let mut state = MarkBuildState::default();
        let codec = KeyCodec::new();
        let (start, end) = codec.prefix_range(KeyPrefix::Extent);

        for pin in roots {
            let reader = self.roots.checkpoint_reader(&pin.root).await?;
            let mut entries = reader.scan(start.clone()..end.clone()).await?;
            while let Some(entry) = entries.next().await? {
                let location = FrameLoc::decode(&entry.value).ok_or_else(|| {
                    GcMarkError::Corrupt(format!(
                        "root {} contains a malformed extent value",
                        pin.root.identity
                    ))
                })?;
                self.push_reference(run_id, digest, location.segid, &mut state)
                    .await?;
            }
            drop(entries);
            reader.close().await?;
        }
        self.finish_observation(run_id, digest, roots.len() as u64, state)
            .await
    }

    async fn push_reference(
        &self,
        run_id: Uuid,
        digest: [u8; 32],
        segid: Segid,
        state: &mut MarkBuildState,
    ) -> Result<(), GcMarkError> {
        let shard = (segid.counter & 0xff) as usize;
        state.buffers[shard].push(segid);
        state.references_enumerated =
            state.references_enumerated.checked_add(1).ok_or_else(|| {
                GcMarkError::Corrupt("enumerated reference count overflow".to_string())
            })?;
        state.buffered = state
            .buffered
            .checked_add(1)
            .ok_or_else(|| GcMarkError::Corrupt("mark buffer count overflow".to_string()))?;
        if state.buffered >= ENUMERATION_BUFFER_SEGMENTS {
            self.flush_buffers(
                run_id,
                digest,
                state.sequence,
                &mut state.buffers,
                &mut state.run_paths,
                &mut state.intermediate_runs,
            )
            .await?;
            state.sequence = state
                .sequence
                .checked_add(1)
                .ok_or_else(|| GcMarkError::Corrupt("mark-run sequence overflow".to_string()))?;
            state.buffered = 0;
        }
        Ok(())
    }

    async fn finish_observation(
        &self,
        run_id: Uuid,
        digest: [u8; 32],
        roots_enumerated: u64,
        mut state: MarkBuildState,
    ) -> Result<GcMarkBuild, GcMarkError> {
        if state.buffered != 0 {
            self.flush_buffers(
                run_id,
                digest,
                state.sequence,
                &mut state.buffers,
                &mut state.run_paths,
                &mut state.intermediate_runs,
            )
            .await?;
        }

        let mut shards = Vec::with_capacity(256);
        let max_write_passes = state
            .run_paths
            .iter()
            .map(OnlineRuns::max_write_passes)
            .max()
            .unwrap_or(0);
        for shard in 0u8..=u8::MAX {
            shards.push(
                self.merge_shard(
                    run_id,
                    digest,
                    shard,
                    std::mem::take(&mut state.run_paths[shard as usize]).into_paths(),
                )
                .await?,
            );
        }
        self.verify_all(run_id, digest, &shards).await?;
        let unique_segments = shards.iter().try_fold(0u64, |total, shard| {
            total
                .checked_add(shard.segment_count)
                .ok_or_else(|| GcMarkError::Corrupt("unique segment count overflow".to_string()))
        })?;
        Ok(GcMarkBuild {
            shards,
            stats: GcMarkStats {
                roots_enumerated,
                references_enumerated: state.references_enumerated,
                intermediate_runs: state.intermediate_runs,
                unique_segments,
                max_write_passes,
            },
        })
    }

    #[cfg(test)]
    async fn build_synthetic_observation<I>(
        &self,
        run_id: Uuid,
        digest: [u8; 32],
        roots_enumerated: u64,
        references: I,
    ) -> Result<GcMarkBuild, GcMarkError>
    where
        I: IntoIterator<Item = Segid>,
    {
        let mut state = MarkBuildState::default();
        for segid in references {
            self.push_reference(run_id, digest, segid, &mut state)
                .await?;
        }
        self.finish_observation(run_id, digest, roots_enumerated, state)
            .await
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
        runs.initial_runs = runs
            .initial_runs
            .checked_add(1)
            .ok_or_else(|| GcMarkError::Corrupt("mark-run count overflow".to_string()))?;
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
    initial_runs: u64,
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

    fn max_write_passes(&self) -> u32 {
        binary_carry_max_write_passes(self.initial_runs)
    }

    #[cfg(test)]
    fn resident_paths(&self) -> usize {
        self.levels.iter().flatten().count()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct MarkResult {
    pub(crate) count: u64,
    pub(crate) checksum: [u8; 32],
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

pub(crate) struct MarkReader {
    reader: BufReader,
    hasher: Sha256,
    remaining: u64,
    expected_count: u64,
    last: Option<Segid>,
}

impl MarkReader {
    pub(crate) async fn open(
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

    pub(crate) async fn next(&mut self) -> Result<Option<Segid>, GcMarkError> {
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

    pub(crate) async fn finish(mut self) -> Result<MarkResult, GcMarkError> {
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

pub(crate) fn decode_digest(value: &str) -> Result<[u8; 32], GcMarkError> {
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

pub(crate) fn encode_digest(value: [u8; 32]) -> String {
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
    use crate::catalog::{GcRootKind, MAX_CHECKPOINTS_PER_BRANCH, MAX_LIVE_BRANCHES};
    use crate::fault_store::FaultStore;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use slatedb::config::{CheckpointOptions, CheckpointScope};
    use std::time::Instant;

    const REPRESENTATIVE_CHECKPOINT_CONFIRMATION: &str =
        "provision-1052672-roots-without-automatic-cleanup";
    const REPRESENTATIVE_MARK_BUDGET: std::time::Duration = std::time::Duration::from_secs(15 * 60);

    fn representative_benchmark_url(
        raw_url: &str,
        confirmation: Option<&str>,
    ) -> Result<url::Url, String> {
        let url: url::Url = raw_url
            .parse()
            .map_err(|error| format!("ZEROFS_GC_BENCHMARK_URL is invalid: {error}"))?;
        if !matches!(
            url.scheme(),
            "s3" | "s3a" | "gs" | "gcs" | "az" | "adl" | "azure" | "abfs" | "abfss" | "https"
        ) {
            return Err(
                "representative qualification rejects local, memory, and plain HTTP stores"
                    .to_string(),
            );
        }
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(
                "benchmark credentials and options must come from the environment, not the URL"
                    .to_string(),
            );
        }
        if confirmation != Some(REPRESENTATIVE_CHECKPOINT_CONFIRMATION) {
            return Err(format!(
                "ZEROFS_GC_BENCHMARK_CONFIRM must equal {REPRESENTATIVE_CHECKPOINT_CONFIRMATION}"
            ));
        }
        Ok(url)
    }

    /// Qualifies the mark buffering, spill, merge, finalization, verification,
    /// and artifact-I/O path at the declared maximum logical root count. The
    /// synthetic reference source deliberately excludes physical checkpoint
    /// open latency, which requires a representative deployed object store.
    #[tokio::test]
    #[ignore = "release-mode supported logical-root mark benchmark"]
    async fn gc_mark_generation_supported_logical_envelope() {
        let root_count = MAX_LIVE_BRANCHES
            .checked_mul(MAX_CHECKPOINTS_PER_BRANCH + 1)
            .unwrap() as u64;
        let (counting, counters) = FaultStore::new(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = counting;
        let marker = GcMarkStore::new(SlateDbRootStore::new(
            Arc::clone(&store),
            Path::from("mark-envelope/branches"),
        ));
        let started = Instant::now();
        let build = marker
            .build_synthetic_observation(
                Uuid::new_v4(),
                [3u8; 32],
                root_count,
                (0..root_count).map(|index| Segid::new(11, index * 256)),
            )
            .await
            .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(build.stats.roots_enumerated, root_count);
        assert_eq!(build.stats.references_enumerated, root_count);
        assert_eq!(build.stats.unique_segments, root_count);
        assert_eq!(build.stats.intermediate_runs, 129);
        assert_eq!(build.stats.max_write_passes, 10);
        assert_eq!(build.shards.len(), 256);
        assert_eq!(build.shards[0].segment_count, root_count);
        assert!(
            build.shards[1..]
                .iter()
                .all(|shard| shard.segment_count == 0)
        );
        let artifact_files =
            counters.put_count() as u64 + counters.multipart_initiate_count() as u64;
        let observed_write_bytes = counters.put_bytes() + counters.multipart_bytes();
        let derived_write_bound = root_count
            .checked_mul(MARK_RECORD_BYTES)
            .and_then(|bytes| bytes.checked_mul(u64::from(build.stats.max_write_passes)))
            .and_then(|bytes| bytes.checked_add(artifact_files * MARK_FILE_OVERHEAD_BYTES))
            .unwrap();
        assert!(observed_write_bytes <= derived_write_bound);
        println!(
            "{}",
            serde_json::json!({
                "logical_roots": root_count,
                "references": build.stats.references_enumerated,
                "unique_segments": build.stats.unique_segments,
                "intermediate_runs": build.stats.intermediate_runs,
                "max_write_passes": build.stats.max_write_passes,
                "derived_write_bound_bytes": derived_write_bound,
                "observed_write_bytes": observed_write_bytes,
                "elapsed_ms": elapsed.as_millis(),
                "object_store": {
                    "gets": counters.get_count(),
                    "puts": counters.put_count(),
                    "lists": counters.list_count(),
                    "get_bytes": counters.get_bytes(),
                    "put_bytes": counters.put_bytes(),
                    "multipart_initiates": counters.multipart_initiate_count(),
                    "multipart_parts": counters.multipart_part_count(),
                    "multipart_completes": counters.multipart_complete_count(),
                    "multipart_bytes": counters.multipart_bytes(),
                },
            })
        );
    }

    /// Expensive manual qualification against the production-class object
    /// store named by `ZEROFS_GC_BENCHMARK_URL`. This provisions the exact
    /// supported physical shape below a unique retained prefix: 4,096 SlateDB
    /// databases with one branch root and 256 checkpoint roots apiece. It does
    /// not clean up automatically so a failed run remains auditable.
    ///
    /// Run only the library target to keep expensive qualification selection
    /// narrow and auditable:
    /// `ZEROFS_GC_BENCHMARK_URL=s3://bucket/prefix \
    ///  ZEROFS_GC_BENCHMARK_CONFIRM=provision-1052672-roots-without-automatic-cleanup \
    ///  cargo test --release --lib gc_physical_checkpoint_open_representative_backend \
    ///  -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "requires an explicitly acknowledged representative object-store target"]
    async fn gc_physical_checkpoint_open_representative_backend() {
        let raw_url = std::env::var("ZEROFS_GC_BENCHMARK_URL")
            .expect("ZEROFS_GC_BENCHMARK_URL must name a representative object-store prefix");
        let confirmation = std::env::var("ZEROFS_GC_BENCHMARK_CONFIRM").ok();
        let url = representative_benchmark_url(&raw_url, confirmation.as_deref())
            .unwrap_or_else(|error| panic!("{error}"));

        let (parsed, configured_prefix) =
            object_store::parse_url_opts(&url, std::env::vars()).unwrap();
        let qualification_id = Uuid::new_v4();
        let qualification_prefix = configured_prefix
            .join("zerofs-gc-physical-open-qualification")
            .join(qualification_id.to_string());
        let qualification_prefix_text = qualification_prefix.to_string();
        let scoped = object_store::prefix::PrefixStore::new(parsed, qualification_prefix);
        let (counting, counters) = FaultStore::new(Arc::new(scoped));
        let store: Arc<dyn ObjectStore> = counting;
        println!(
            "{}",
            serde_json::json!({
                "event": "fixture_started",
                "backend_scheme": url.scheme(),
                "qualification_id": qualification_id,
                "retained_object_prefix": qualification_prefix_text,
                "automatic_cleanup": false,
            })
        );

        let fixture_started = Instant::now();
        let branch_pin_sets = futures::stream::iter(0..MAX_LIVE_BRANCHES)
            .map(|branch_index| {
                let store = Arc::clone(&store);
                let database_path = Path::from("databases").join(format!("{branch_index:04x}"));
                async move {
                    let db = Db::builder(database_path.clone(), store)
                        .with_segment_extractor(Arc::new(
                            crate::segment_extractor::ZeroFsSegmentExtractor,
                        ))
                        .build()
                        .await
                        .unwrap();
                    db.put(
                        &KeyCodec::new().extent_key(1, 0),
                        &FrameLoc {
                            segid: Segid::new(branch_index as u64 + 1, 0),
                            frame_index: 0,
                            byte_offset: 0,
                            byte_len: 1,
                        }
                        .encode(),
                    )
                    .await
                    .unwrap();
                    db.flush().await.unwrap();
                    let mut pins = Vec::with_capacity(MAX_CHECKPOINTS_PER_BRANCH + 1);
                    for root_index in 0..=MAX_CHECKPOINTS_PER_BRANCH {
                        let checkpoint = db
                            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
                            .await
                            .unwrap();
                        pins.push(GcRootPin {
                            kind: if root_index == 0 {
                                GcRootKind::Branch
                            } else {
                                GcRootKind::Checkpoint
                            },
                            root: crate::catalog::ImmutableCheckpoint {
                                database_path: database_path.clone(),
                                checkpoint_id: checkpoint.id,
                                manifest_id: checkpoint.manifest_id,
                            }
                            .durable_root(),
                        });
                    }
                    db.close().await.unwrap();
                    pins
                }
            })
            .buffer_unordered(16)
            .collect::<Vec<_>>()
            .await;
        let mut pins = branch_pin_sets.into_iter().flatten().collect::<Vec<_>>();
        pins.sort_by(|left, right| {
            (&left.root.identity, &left.root.manifest_id)
                .cmp(&(&right.root.identity, &right.root.manifest_id))
        });
        let expected_roots = MAX_LIVE_BRANCHES
            .checked_mul(MAX_CHECKPOINTS_PER_BRANCH + 1)
            .unwrap();
        assert_eq!(pins.len(), expected_roots);
        let fixture_elapsed = fixture_started.elapsed();

        counters.reset_counts();
        let marker = GcMarkStore::new(SlateDbRootStore::new(
            Arc::clone(&store),
            Path::from("branch-roots"),
        ));
        let mark_started = Instant::now();
        let build = match tokio::time::timeout(
            REPRESENTATIVE_MARK_BUDGET,
            marker.build_observation(Uuid::new_v4(), &"07".repeat(32), &pins),
        )
        .await
        {
            Ok(result) => result.unwrap(),
            Err(_) => {
                let mark_elapsed = mark_started.elapsed();
                println!(
                    "{}",
                    serde_json::json!({
                        "event": "qualification_failed",
                        "reason": "mark_timeout",
                        "backend_scheme": url.scheme(),
                        "qualification_id": qualification_id,
                        "retained_object_prefix": qualification_prefix_text,
                        "automatic_cleanup": false,
                        "physical_roots": expected_roots,
                        "fixture_ms": fixture_elapsed.as_millis(),
                        "mark_ms": mark_elapsed.as_millis(),
                        "mark_budget_ms": REPRESENTATIVE_MARK_BUDGET.as_millis(),
                        "object_store": {
                            "gets": counters.get_count(),
                            "puts": counters.put_count(),
                            "lists": counters.list_count(),
                            "get_bytes": counters.get_bytes(),
                            "put_bytes": counters.put_bytes(),
                            "multipart_initiates": counters.multipart_initiate_count(),
                            "multipart_parts": counters.multipart_part_count(),
                            "multipart_completes": counters.multipart_complete_count(),
                            "multipart_bytes": counters.multipart_bytes(),
                        },
                    })
                );
                panic!(
                    "physical checkpoint mark exceeded the {REPRESENTATIVE_MARK_BUDGET:?} timeout"
                );
            }
        };
        let mark_elapsed = mark_started.elapsed();
        assert_eq!(build.stats.roots_enumerated, expected_roots as u64);
        assert_eq!(build.stats.references_enumerated, expected_roots as u64);
        assert_eq!(build.stats.unique_segments, MAX_LIVE_BRANCHES as u64);
        println!(
            "{}",
            serde_json::json!({
                "event": "qualification_completed",
                "backend_scheme": url.scheme(),
                "qualification_id": qualification_id,
                "retained_object_prefix": qualification_prefix_text,
                "automatic_cleanup": false,
                "branches": MAX_LIVE_BRANCHES,
                "checkpoints_per_branch": MAX_CHECKPOINTS_PER_BRANCH,
                "physical_roots": expected_roots,
                "references": build.stats.references_enumerated,
                "unique_segments": build.stats.unique_segments,
                "fixture_ms": fixture_elapsed.as_millis(),
                "mark_ms": mark_elapsed.as_millis(),
                "mark_budget_ms": REPRESENTATIVE_MARK_BUDGET.as_millis(),
                "object_store": {
                    "gets": counters.get_count(),
                    "puts": counters.put_count(),
                    "lists": counters.list_count(),
                    "get_bytes": counters.get_bytes(),
                    "put_bytes": counters.put_bytes(),
                    "multipart_initiates": counters.multipart_initiate_count(),
                    "multipart_parts": counters.multipart_part_count(),
                    "multipart_completes": counters.multipart_complete_count(),
                    "multipart_bytes": counters.multipart_bytes(),
                },
            })
        );
    }

    #[test]
    fn representative_backend_guard_requires_remote_secret_free_acknowledged_target() {
        assert!(
            representative_benchmark_url(
                "s3://qualification-bucket/prefix",
                Some(REPRESENTATIVE_CHECKPOINT_CONFIRMATION),
            )
            .is_ok()
        );
        for url in [
            "memory:///prefix",
            "file:///tmp/prefix",
            "http://example.com/prefix",
            "s3://user:secret@qualification-bucket/prefix",
            "s3://qualification-bucket/prefix?secret=value",
            "s3://qualification-bucket/prefix#fragment",
        ] {
            assert!(
                representative_benchmark_url(url, Some(REPRESENTATIVE_CHECKPOINT_CONFIRMATION))
                    .is_err(),
                "unsafe qualification target unexpectedly accepted: {url}"
            );
        }
        assert!(representative_benchmark_url("s3://qualification-bucket/prefix", None).is_err());
        assert!(
            representative_benchmark_url("s3://qualification-bucket/prefix", Some("wrong"))
                .is_err()
        );
    }

    #[test]
    fn binary_carry_write_pass_bound_covers_merge_level_transitions() {
        assert_eq!(binary_carry_max_write_passes(0), 0);
        assert_eq!(binary_carry_max_write_passes(1), 2);
        assert_eq!(binary_carry_max_write_passes(2), 3);
        assert_eq!(binary_carry_max_write_passes(3), 4);
        assert_eq!(binary_carry_max_write_passes(4), 4);
        assert_eq!(binary_carry_max_write_passes(5), 5);
        assert_eq!(binary_carry_max_write_passes(7), 6);
        assert_eq!(binary_carry_max_write_passes(8), 5);
        assert_eq!(binary_carry_max_write_passes(1_000_000), 24);
        assert_eq!(binary_carry_global_write_pass_upper_bound(0), 0);
        assert_eq!(binary_carry_global_write_pass_upper_bound(1), 2);
        assert_eq!(binary_carry_global_write_pass_upper_bound(7), 6);
        assert_eq!(binary_carry_global_write_pass_upper_bound(8), 7);
        assert_eq!(binary_carry_global_write_pass_upper_bound(15), 7);
        assert_eq!(binary_carry_global_write_pass_upper_bound(16), 9);
    }

    fn raw_mark_file(run_id: Uuid, digest: [u8; 32], shard: u8, values: &[Segid]) -> Vec<u8> {
        let mut bytes = mark_header(run_id, digest, shard).to_vec();
        for value in values {
            bytes.extend_from_slice(&encode_segid(*value));
        }
        bytes.extend_from_slice(&(values.len() as u64).to_le_bytes());
        let checksum: [u8; 32] = Sha256::digest(&bytes).into();
        bytes.extend_from_slice(&checksum);
        bytes
    }

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

    #[tokio::test]
    async fn reader_rejects_missing_corrupt_truncated_duplicate_and_reordered_shards() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let run_id = Uuid::new_v4();
        let digest = [7u8; 32];
        let shard = 0;
        let missing = Path::from("marks/missing");
        assert!(matches!(
            MarkReader::open(Arc::clone(&store), &missing, run_id, digest, shard).await,
            Err(GcMarkError::ObjectStore(
                object_store::Error::NotFound { .. }
            ))
        ));

        let truncated = Path::from("marks/truncated");
        store
            .put(&truncated, bytes::Bytes::from_static(b"short").into())
            .await
            .unwrap();
        assert!(matches!(
            MarkReader::open(Arc::clone(&store), &truncated, run_id, digest, shard).await,
            Err(GcMarkError::Corrupt(_))
        ));

        let one = Segid::new(1, 0);
        let two = Segid::new(1, 256);
        let wrong_identity = Path::from("marks/wrong-identity");
        let mut wrong_identity_bytes = raw_mark_file(run_id, digest, shard, &[one]);
        wrong_identity_bytes[0] ^= 0xff;
        store
            .put(&wrong_identity, wrong_identity_bytes.into())
            .await
            .unwrap();
        assert!(matches!(
            MarkReader::open(Arc::clone(&store), &wrong_identity, run_id, digest, shard,).await,
            Err(GcMarkError::Corrupt(_))
        ));

        let bad_checksum = Path::from("marks/bad-checksum");
        let mut bad_checksum_bytes = raw_mark_file(run_id, digest, shard, &[one]);
        *bad_checksum_bytes.last_mut().unwrap() ^= 0xff;
        store
            .put(&bad_checksum, bad_checksum_bytes.into())
            .await
            .unwrap();
        let mut reader = MarkReader::open(Arc::clone(&store), &bad_checksum, run_id, digest, shard)
            .await
            .unwrap();
        assert_eq!(reader.next().await.unwrap(), Some(one));
        assert!(matches!(
            reader.finish().await,
            Err(GcMarkError::Corrupt(_))
        ));

        for (name, values) in [("duplicate", vec![one, one]), ("reordered", vec![two, one])] {
            let path = Path::from(format!("marks/{name}"));
            store
                .put(&path, raw_mark_file(run_id, digest, shard, &values).into())
                .await
                .unwrap();
            let mut reader = MarkReader::open(Arc::clone(&store), &path, run_id, digest, shard)
                .await
                .unwrap();
            assert_eq!(reader.next().await.unwrap(), Some(values[0]));
            assert!(matches!(reader.next().await, Err(GcMarkError::Corrupt(_))));
        }
    }
}
