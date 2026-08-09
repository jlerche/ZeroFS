//! Object-store-backed segment writer/reader (RFC-0025).
//!
//! Seals a batch of extent frames into one immutable
//! `segments/<shard>/<epoch>/<counter>` object (durable on return) and reads a
//! single extent back by its [`FrameLoc`].
//! The counter is per-instance (reset each process open); epoch namespacing is
//! what keeps two writer terms from colliding on an object key.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use slatedb::object_store::{
    CopyMode, CopyOptions, GetOptions, GetRange, MultipartUpload, ObjectStore, ObjectStoreExt,
    PutMode, PutOptions, path::Path,
};

use crate::frame_codec::{Compressed, FrameCodec};
use crate::fs::inode::InodeId;
use crate::segment::{
    DirEntry, FOOTER_LEN, FrameLoc, LEN_PREFIX, Segid, SegmentBuilder, SegmentError,
    seal_compressed_batch,
};

const SEGMENT_EPOCH_PREFIX: &str = "segment-epochs";
const LEGACY_SEGMENT_EPOCH_RESERVATION_VERSION: u32 = 1;
const SEGMENT_EPOCH_RESERVATION_VERSION: u32 = 2;
const SEGMENT_POOL_GENESIS_PATH: &str = "segment-pool-genesis.json";
const LEGACY_SEGMENT_POOL_GENESIS_VERSION: u32 = 1;
const SEGMENT_POOL_GENESIS_VERSION: u32 = 2;
const SEGMENT_POOL_AUTH_INFO: &[u8] = b"zerofs-v1-segment-pool-authority";
const SEGMENT_UPLOAD_PREFIX: &str = "segment-uploads";
const PRIVATE_GC_ARTIFACT_PREFIX: &str = "private-gc-artifacts";
const LEGACY_MIGRATION_PREFIX: &str = "legacy-segment-migrations";
const LEGACY_SEGMENT_CLAIM_PREFIX: &str = "legacy-segment-claims";
const LEGACY_MIGRATION_BOOTSTRAP_PREFIX: &str = "legacy-migration-bootstraps";
const LEGACY_MIGRATION_VERSION: u32 = 1;

#[derive(serde::Deserialize, serde::Serialize)]
struct SegmentPoolGenesis {
    schema_version: u32,
    pool_id: uuid::Uuid,
    creator_database_identity: String,
    #[serde(default)]
    legacy_source: Option<LegacyMigrationSource>,
    auth_tag: [u8; 32],
}

#[derive(Clone)]
pub struct SegmentPoolAuthority {
    pool_id: uuid::Uuid,
    auth_key: [u8; 32],
    legacy_source: Option<LegacyMigrationSource>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct LegacyMigrationSource {
    database_identity: String,
    database_instance_id: uuid::Uuid,
    wrapped_key_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct LegacyMigrationBootstrap {
    schema_version: u32,
    database_identity: String,
    database_instance_id: uuid::Uuid,
}

/// Exact storage-authenticated identity of one branch-owned writer epoch.
/// Callers must still consult authoritative lifecycle state before treating
/// any segment in the epoch as private or reclaimable.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // The standalone binary does not yet open the branch catalog lifecycle.
pub struct AuthenticatedBranchEpoch {
    pub pool_id: uuid::Uuid,
    pub epoch: u64,
    pub reservation_id: uuid::Uuid,
    pub database_identity: String,
    pub branch_id: uuid::Uuid,
}

/// Exact immutable object identity captured before a local deletion attempt.
/// A later worker must stream and match every field again immediately before
/// deleting the object.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // Consumed by the disabled private-epoch collector path.
pub(crate) struct SegmentObjectIdentity {
    pub(crate) segid: Segid,
    pub(crate) size: u64,
    pub(crate) modified_seconds: i64,
    pub(crate) modified_nanos: u32,
    pub(crate) content_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct SegmentEpochReservation {
    schema_version: u32,
    pool_id: uuid::Uuid,
    epoch: u64,
    reservation_id: uuid::Uuid,
    database_identity: String,
    #[serde(default)]
    branch_id: Option<uuid::Uuid>,
    auth_tag: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct LegacyMigrationIntent {
    schema_version: u32,
    pool_id: uuid::Uuid,
    migration_id: uuid::Uuid,
    database_identity: String,
    database_instance_id: uuid::Uuid,
    wrapped_key_digest: [u8; 32],
    initial_segment_count: u64,
    initial_total_bytes: u64,
    initial_inventory_fingerprint: [u8; 32],
    auth_tag: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct LegacySegmentClaim {
    schema_version: u32,
    pool_id: uuid::Uuid,
    migration_id: uuid::Uuid,
    database_identity: String,
    database_instance_id: uuid::Uuid,
    wrapped_key_digest: [u8; 32],
    segid: Segid,
    size: u64,
    content_digest: [u8; 32],
    auth_tag: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct LegacyMigrationCompletion {
    schema_version: u32,
    pool_id: uuid::Uuid,
    migration_id: uuid::Uuid,
    database_identity: String,
    database_instance_id: uuid::Uuid,
    wrapped_key_digest: [u8; 32],
    segment_count: u64,
    total_bytes: u64,
    inventory_fingerprint: [u8; 32],
    auth_tag: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyMigrationReport {
    pub migration_id: uuid::Uuid,
    pub segment_count: u64,
    pub total_bytes: u64,
    pub inventory_fingerprint: [u8; 32],
}

#[derive(Default)]
struct LegacyInventory {
    segment_count: u64,
    total_bytes: u64,
    fingerprint: [u8; 32],
}

impl LegacyInventory {
    fn include(&mut self, segid: Segid, size: u64, content_digest: [u8; 32]) -> Result<()> {
        self.segment_count = self.segment_count.checked_add(1).ok_or_else(|| {
            SegmentStoreError::ObjectStore("legacy migration segment count overflow".to_string())
        })?;
        self.total_bytes = self.total_bytes.checked_add(size).ok_or_else(|| {
            SegmentStoreError::ObjectStore("legacy migration byte count overflow".to_string())
        })?;
        let item = Sha256::digest(
            [
                b"legacy-inventory-item".as_slice(),
                &segid.epoch.to_be_bytes(),
                &segid.counter.to_be_bytes(),
                &size.to_be_bytes(),
                &content_digest,
            ]
            .concat(),
        );
        for (accumulator, byte) in self.fingerprint.iter_mut().zip(item) {
            *accumulator ^= byte;
        }
        Ok(())
    }
}

fn derive_pool_auth_key(master_key: &[u8; 32]) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(None, master_key);
    let mut key = [0u8; 32];
    hk.expand(SEGMENT_POOL_AUTH_INFO, &mut key)
        .expect("valid HKDF output length");
    key
}

fn authentication_tag(key: &[u8; 32], fields: &[&[u8]]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts a 32-byte key");
    for field in fields {
        mac.update(&(field.len() as u64).to_be_bytes());
        mac.update(field);
    }
    mac.finalize().into_bytes().into()
}

fn authentication_tag_is_valid(key: &[u8; 32], fields: &[&[u8]], tag: &[u8; 32]) -> bool {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts a 32-byte key");
    for field in fields {
        mac.update(&(field.len() as u64).to_be_bytes());
        mac.update(field);
    }
    mac.verify_slice(tag).is_ok()
}

fn genesis_tag(
    key: &[u8; 32],
    schema_version: u32,
    pool_id: uuid::Uuid,
    database_identity: &str,
    legacy_source: Option<&LegacyMigrationSource>,
) -> [u8; 32] {
    let absent_uuid = [0u8; 16];
    let absent_digest = [0u8; 32];
    authentication_tag(
        key,
        &[
            b"genesis",
            &schema_version.to_be_bytes(),
            pool_id.as_bytes(),
            database_identity.as_bytes(),
            legacy_source.map_or(&[][..], |source| source.database_identity.as_bytes()),
            legacy_source.map_or(absent_uuid.as_slice(), |source| {
                source.database_instance_id.as_bytes()
            }),
            legacy_source.map_or(absent_digest.as_slice(), |source| {
                source.wrapped_key_digest.as_slice()
            }),
        ],
    )
}

#[cfg(test)]
fn reservation_tag_v1(
    authority: &SegmentPoolAuthority,
    epoch: u64,
    reservation_id: uuid::Uuid,
    database_identity: &str,
) -> [u8; 32] {
    authentication_tag(
        &authority.auth_key,
        &[
            b"epoch",
            &LEGACY_SEGMENT_EPOCH_RESERVATION_VERSION.to_be_bytes(),
            authority.pool_id.as_bytes(),
            &epoch.to_be_bytes(),
            reservation_id.as_bytes(),
            database_identity.as_bytes(),
        ],
    )
}

fn reservation_tag(
    authority: &SegmentPoolAuthority,
    epoch: u64,
    reservation_id: uuid::Uuid,
    database_identity: &str,
    branch_id: Option<uuid::Uuid>,
) -> [u8; 32] {
    let absent = [0u8; 16];
    let branch = branch_id
        .as_ref()
        .map_or(absent.as_slice(), |id| id.as_bytes().as_slice());
    authentication_tag(
        &authority.auth_key,
        &[
            b"epoch",
            &SEGMENT_EPOCH_RESERVATION_VERSION.to_be_bytes(),
            authority.pool_id.as_bytes(),
            &epoch.to_be_bytes(),
            reservation_id.as_bytes(),
            database_identity.as_bytes(),
            &[u8::from(branch_id.is_some())],
            branch,
        ],
    )
}

fn reservation_tag_is_valid(
    authority: &SegmentPoolAuthority,
    marker: &SegmentEpochReservation,
) -> bool {
    match marker.schema_version {
        LEGACY_SEGMENT_EPOCH_RESERVATION_VERSION if marker.branch_id.is_none() => {
            authentication_tag_is_valid(
                &authority.auth_key,
                &[
                    b"epoch",
                    &LEGACY_SEGMENT_EPOCH_RESERVATION_VERSION.to_be_bytes(),
                    authority.pool_id.as_bytes(),
                    &marker.epoch.to_be_bytes(),
                    marker.reservation_id.as_bytes(),
                    marker.database_identity.as_bytes(),
                ],
                &marker.auth_tag,
            )
        }
        SEGMENT_EPOCH_RESERVATION_VERSION => {
            let absent = [0u8; 16];
            let branch = marker
                .branch_id
                .as_ref()
                .map_or(absent.as_slice(), |id| id.as_bytes().as_slice());
            authentication_tag_is_valid(
                &authority.auth_key,
                &[
                    b"epoch",
                    &SEGMENT_EPOCH_RESERVATION_VERSION.to_be_bytes(),
                    authority.pool_id.as_bytes(),
                    &marker.epoch.to_be_bytes(),
                    marker.reservation_id.as_bytes(),
                    marker.database_identity.as_bytes(),
                    &[u8::from(marker.branch_id.is_some())],
                    branch,
                ],
                &marker.auth_tag,
            )
        }
        _ => false,
    }
}

fn legacy_migration_id(authority: &SegmentPoolAuthority, database_identity: &str) -> uuid::Uuid {
    let digest = authentication_tag(
        &authority.auth_key,
        &[
            b"legacy-migration-id",
            authority.pool_id.as_bytes(),
            database_identity.as_bytes(),
        ],
    );
    let mut bytes: [u8; 16] = digest[..16].try_into().expect("digest prefix is 16 bytes");
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

fn legacy_intent_tag(authority: &SegmentPoolAuthority, intent: &LegacyMigrationIntent) -> [u8; 32] {
    authentication_tag(
        &authority.auth_key,
        &[
            b"legacy-migration-intent",
            &LEGACY_MIGRATION_VERSION.to_be_bytes(),
            authority.pool_id.as_bytes(),
            intent.migration_id.as_bytes(),
            intent.database_identity.as_bytes(),
            intent.database_instance_id.as_bytes(),
            &intent.wrapped_key_digest,
            &intent.initial_segment_count.to_be_bytes(),
            &intent.initial_total_bytes.to_be_bytes(),
            &intent.initial_inventory_fingerprint,
        ],
    )
}

fn legacy_claim_tag(authority: &SegmentPoolAuthority, claim: &LegacySegmentClaim) -> [u8; 32] {
    authentication_tag(
        &authority.auth_key,
        &[
            b"legacy-segment-claim",
            &claim.schema_version.to_be_bytes(),
            claim.pool_id.as_bytes(),
            claim.migration_id.as_bytes(),
            claim.database_identity.as_bytes(),
            claim.database_instance_id.as_bytes(),
            &claim.wrapped_key_digest,
            &claim.segid.epoch.to_be_bytes(),
            &claim.segid.counter.to_be_bytes(),
            &claim.size.to_be_bytes(),
            &claim.content_digest,
        ],
    )
}

fn legacy_completion_tag(
    authority: &SegmentPoolAuthority,
    completion: &LegacyMigrationCompletion,
) -> [u8; 32] {
    authentication_tag(
        &authority.auth_key,
        &[
            b"legacy-migration-completion",
            &completion.schema_version.to_be_bytes(),
            completion.pool_id.as_bytes(),
            completion.migration_id.as_bytes(),
            completion.database_identity.as_bytes(),
            completion.database_instance_id.as_bytes(),
            &completion.wrapped_key_digest,
            &completion.segment_count.to_be_bytes(),
            &completion.total_bytes.to_be_bytes(),
            &completion.inventory_fingerprint,
        ],
    )
}

fn legacy_intent_path(migration_id: uuid::Uuid) -> Path {
    Path::from(format!(
        "{LEGACY_MIGRATION_PREFIX}/{migration_id}/intent.json"
    ))
}

fn legacy_claim_path(segid: Segid) -> Path {
    Path::from(format!(
        "{LEGACY_SEGMENT_CLAIM_PREFIX}/{:02x}/{:016x}/{:016x}.json",
        segid.counter & 0xff,
        segid.epoch,
        segid.counter
    ))
}

fn legacy_completion_path(migration_id: uuid::Uuid) -> Path {
    Path::from(format!(
        "{LEGACY_MIGRATION_PREFIX}/{migration_id}/completion.json"
    ))
}

fn legacy_bootstrap_path(database_instance_id: uuid::Uuid) -> Path {
    Path::from(format!(
        "{LEGACY_MIGRATION_BOOTSTRAP_PREFIX}/{database_instance_id}.json"
    ))
}

async fn load_pool_authority(
    object_store: &Arc<dyn ObjectStore>,
    auth_key: [u8; 32],
) -> Result<Option<SegmentPoolAuthority>> {
    let bytes = match object_store
        .get(&Path::from(SEGMENT_POOL_GENESIS_PATH))
        .await
    {
        Ok(result) => result
            .bytes()
            .await
            .map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?,
        Err(slatedb::object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(SegmentStoreError::ObjectStore(error.to_string())),
    };
    let genesis: SegmentPoolGenesis = serde_json::from_slice(&bytes).map_err(|error| {
        SegmentStoreError::ObjectStore(format!("invalid shared segment-pool genesis: {error}"))
    })?;
    let valid_tag = match genesis.schema_version {
        LEGACY_SEGMENT_POOL_GENESIS_VERSION => {
            genesis.legacy_source.is_none()
                && authentication_tag_is_valid(
                    &auth_key,
                    &[
                        b"genesis",
                        &LEGACY_SEGMENT_POOL_GENESIS_VERSION.to_be_bytes(),
                        genesis.pool_id.as_bytes(),
                        genesis.creator_database_identity.as_bytes(),
                    ],
                    &genesis.auth_tag,
                )
        }
        SEGMENT_POOL_GENESIS_VERSION => {
            genesis.auth_tag
                == genesis_tag(
                    &auth_key,
                    genesis.schema_version,
                    genesis.pool_id,
                    &genesis.creator_database_identity,
                    genesis.legacy_source.as_ref(),
                )
        }
        _ => false,
    };
    if genesis.pool_id.is_nil() || !valid_tag {
        return Err(SegmentStoreError::ObjectStore(
            "shared segment-pool genesis is unauthenticated or unsupported".to_string(),
        ));
    }
    Ok(Some(SegmentPoolAuthority {
        pool_id: genesis.pool_id,
        auth_key,
        legacy_source: genesis.legacy_source,
    }))
}

#[derive(Debug, thiserror::Error)]
pub enum SegmentStoreError {
    #[error("segment object store error: {0}")]
    ObjectStore(String),
    /// The segment object does not exist. Distinguished from a transient
    /// `ObjectStore` error so reclamation can tell "the object is genuinely gone"
    /// (trivially dead) from "the store is momentarily unreachable" (fail-closed).
    #[error("segment object not found")]
    NotFound,
    #[error(transparent)]
    Segment(#[from] SegmentError),
}

type Result<T> = std::result::Result<T, SegmentStoreError>;

/// Concurrent per-shard LIST chains inside [`SegmentStore::list_segments_stream`].
/// Bounds in-flight LIST requests and, with them, how much listing a retrying
/// layer below buffers at once.
const LIST_SHARD_CONCURRENCY: usize = 16;

/// Concurrent multipart part uploads per sealing segment. Higher than the
/// throughput-saturating default of 8 because per-seal wall time is fsync tail
/// latency (the durability barrier waits out in-flight seals). Parts are
/// zero-copy slices of the seal bytes, so extra concurrency costs requests in
/// flight, not buffer copies.
const SEAL_UPLOAD_CONCURRENCY: usize = 16;

/// Multipart part size, and the seal size at which `put_segment` switches from
/// a single PUT to multipart.
const SEAL_PART_SIZE: usize = 10 * 1024 * 1024;

/// Warm a just-written segment into the read (parts) cache: the multipart
/// upload bypasses the store's single-PUT write-through, so `put_segment`
/// calls this with the bytes it already holds. The hook applies any
/// object-store prefix itself. `None` when there is no such cache (tests).
pub type SegmentWarmHook = Arc<dyn Fn(&Path, Bytes) + Send + Sync>;

/// Writes and reads `segments/` objects against an object store.
pub struct SegmentStore {
    object_store: Arc<dyn ObjectStore>,
    codec: Arc<FrameCodec>,
    epoch: u64,
    counter: AtomicU64,
    /// Count of ranged segment GETs issued (a read-amplification metric).
    read_calls: AtomicU64,
    /// Warms the parts cache with a just-written segment (see
    /// [`SegmentWarmHook`]); that cache is the only segment cache.
    warm: Option<SegmentWarmHook>,
}

fn legacy_relative_segment_key(database_path: &Path, location: &Path) -> Result<(String, Segid)> {
    let database = database_path.to_string();
    let location = location.to_string();
    let relative = if database.is_empty() {
        location.as_str()
    } else {
        location
            .strip_prefix(&database)
            .and_then(|suffix| suffix.strip_prefix('/'))
            .ok_or_else(|| {
                SegmentStoreError::ObjectStore(format!(
                    "legacy segment {location} is outside database {database}"
                ))
            })?
    };
    let segid = Segid::from_object_key(relative)
        .filter(|segid| segid.object_key() == relative)
        .ok_or_else(|| {
            SegmentStoreError::ObjectStore(format!(
                "legacy database contains a noncanonical segment key: {location}"
            ))
        })?;
    Ok((relative.to_string(), segid))
}

fn path_below(parent: &Path, relative: &str) -> Path {
    if parent.as_ref().is_empty() {
        Path::from(relative)
    } else {
        Path::from(format!("{parent}/{relative}"))
    }
}

async fn digest_object(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
) -> Result<(u64, [u8; 32])> {
    let result = object_store
        .get(path)
        .await
        .map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?;
    let expected_size = result.meta.size;
    let mut stream = result.into_stream();
    let mut size = 0u64;
    let mut hasher = Sha256::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?;
        size = size.checked_add(chunk.len() as u64).ok_or_else(|| {
            SegmentStoreError::ObjectStore("object digest length overflow".to_string())
        })?;
        hasher.update(&chunk);
    }
    if size != expected_size {
        return Err(SegmentStoreError::ObjectStore(format!(
            "object {path} body length disagrees with metadata"
        )));
    }
    Ok((size, hasher.finalize().into()))
}

async fn verify_segment_key_matches_footer(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
    expected: Segid,
) -> Result<()> {
    let result = object_store
        .get_opts(
            path,
            GetOptions {
                range: Some(GetRange::Suffix(FOOTER_LEN as u64)),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?;
    let object_size = result.meta.size;
    let footer = result
        .bytes()
        .await
        .map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?;
    let actual = crate::segment::parse_footer(&footer, object_size)?.segid;
    if actual != expected {
        return Err(SegmentError::SegidMismatch {
            expected,
            found: actual,
        }
        .into());
    }
    Ok(())
}

async fn load_json<T: serde::de::DeserializeOwned>(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
) -> Result<Option<T>> {
    let result = match object_store.get(path).await {
        Ok(result) => result,
        Err(slatedb::object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(SegmentStoreError::ObjectStore(error.to_string())),
    };
    let bytes = result
        .bytes()
        .await
        .map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))
}

async fn create_json_exact<T>(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
    record: &T,
) -> Result<bool>
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq,
{
    let payload = serde_json::to_vec(record)
        .map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?;
    match object_store
        .put_opts(path, payload.into(), PutOptions::from(PutMode::Create))
        .await
    {
        Ok(_) => Ok(true),
        Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
            let existing = load_json::<T>(object_store, path).await?.ok_or_else(|| {
                SegmentStoreError::ObjectStore(format!(
                    "immutable object {path} disappeared after create conflict"
                ))
            })?;
            if existing == *record {
                Ok(false)
            } else {
                Err(SegmentStoreError::ObjectStore(format!(
                    "immutable object {path} conflicts with this migration"
                )))
            }
        }
        Err(error) => Err(SegmentStoreError::ObjectStore(error.to_string())),
    }
}

impl SegmentStore {
    /// Create a fresh writer term over the same authenticated pool and codec.
    /// The caller must have already reserved `epoch`; counters restart at zero
    /// only because the epoch namespace is permanent and never reused.
    #[allow(dead_code)] // The standalone binary does not yet open the branch catalog lifecycle.
    pub(crate) fn rotated(&self, epoch: u64) -> Self {
        Self {
            object_store: Arc::clone(&self.object_store),
            codec: Arc::clone(&self.codec),
            epoch,
            counter: AtomicU64::new(0),
            read_calls: AtomicU64::new(0),
            warm: self.warm.clone(),
        }
    }

    /// Open the authenticated authority for an existing pool, or establish it
    /// with an immutable create only while the pool contains no data-plane or
    /// control-plane object other than its newly initialized wrapped key.
    pub async fn open_or_create_pool_authority(
        object_store: Arc<dyn ObjectStore>,
        master_key: &[u8; 32],
        database_identity: &str,
        allow_create: bool,
    ) -> Result<SegmentPoolAuthority> {
        Self::open_or_create_pool_authority_inner(
            object_store,
            master_key,
            database_identity,
            allow_create,
            None,
        )
        .await
    }

    /// Publish a create-only fail-closed bootstrap before copying a legacy key
    /// into a fresh pool. If the command crashes before authenticated genesis,
    /// ordinary startup refuses to reinterpret the half-created pool as native.
    pub async fn mark_legacy_pool_bootstrap(
        pool_store: &Arc<dyn ObjectStore>,
        database_identity: &str,
        database_instance_id: uuid::Uuid,
    ) -> Result<()> {
        if database_identity.is_empty()
            || database_identity.len() > 4 * 1024
            || database_instance_id.is_nil()
        {
            return Err(SegmentStoreError::ObjectStore(
                "legacy bootstrap requires a bounded path and non-nil database identity"
                    .to_string(),
            ));
        }
        create_json_exact(
            pool_store,
            &legacy_bootstrap_path(database_instance_id),
            &LegacyMigrationBootstrap {
                schema_version: LEGACY_MIGRATION_VERSION,
                database_identity: database_identity.to_string(),
                database_instance_id,
            },
        )
        .await?;
        Ok(())
    }

    /// Establish the first pool genesis with an authenticated, non-clearable
    /// requirement for this exact legacy database incarnation to complete its
    /// import before serving. Existing pools use an authenticated intent for
    /// each additional legacy source.
    pub async fn open_or_create_legacy_pool_authority(
        object_store: Arc<dyn ObjectStore>,
        master_key: &[u8; 32],
        database_identity: &str,
        database_instance_id: uuid::Uuid,
        wrapped_key_digest: [u8; 32],
    ) -> Result<SegmentPoolAuthority> {
        if database_instance_id.is_nil() {
            return Err(SegmentStoreError::ObjectStore(
                "legacy database instance identity must be non-nil".to_string(),
            ));
        }
        let source = LegacyMigrationSource {
            database_identity: database_identity.to_string(),
            database_instance_id,
            wrapped_key_digest,
        };
        Self::open_or_create_pool_authority_inner(
            object_store,
            master_key,
            database_identity,
            true,
            Some(source),
        )
        .await
    }

    async fn open_or_create_pool_authority_inner(
        object_store: Arc<dyn ObjectStore>,
        master_key: &[u8; 32],
        database_identity: &str,
        allow_create: bool,
        legacy_source: Option<LegacyMigrationSource>,
    ) -> Result<SegmentPoolAuthority> {
        if database_identity.len() > 4 * 1024 {
            return Err(SegmentStoreError::ObjectStore(
                "segment-pool database identity must contain at most 4096 bytes".to_string(),
            ));
        }
        let auth_key = derive_pool_auth_key(master_key);
        if let Some(authority) = load_pool_authority(&object_store, auth_key).await? {
            return Ok(authority);
        }
        if !allow_create {
            return Err(SegmentStoreError::ObjectStore(
                "shared segment-pool genesis is absent and read-only startup cannot create it"
                    .to_string(),
            ));
        }

        let mut objects = object_store.list(None);
        let mut has_wrapped_key = false;
        while let Some(object) = objects.next().await {
            let object =
                object.map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?;
            has_wrapped_key |= object.location.as_ref() == "zerofs.key";
            let allowed_bootstrap = if legacy_source.as_ref().is_some_and(|source| {
                object.location == legacy_bootstrap_path(source.database_instance_id)
            }) {
                if let Some(source) = legacy_source.as_ref() {
                    load_json::<LegacyMigrationBootstrap>(&object_store, &object.location)
                        .await?
                        .is_some_and(|bootstrap| {
                            bootstrap.schema_version == LEGACY_MIGRATION_VERSION
                                && bootstrap.database_identity == source.database_identity
                                && bootstrap.database_instance_id == source.database_instance_id
                        })
                } else {
                    false
                }
            } else {
                false
            };
            if object.location.as_ref() != "zerofs.key" && !allowed_bootstrap {
                return Err(SegmentStoreError::ObjectStore(format!(
                    "cannot establish shared segment-pool genesis because {} already exists",
                    object.location
                )));
            }
        }
        if legacy_source.is_some() && !has_wrapped_key {
            return Err(SegmentStoreError::ObjectStore(
                "cannot establish legacy shared-pool genesis before its exact wrapped key exists"
                    .to_string(),
            ));
        }

        let pool_id = uuid::Uuid::new_v4();
        let genesis = SegmentPoolGenesis {
            schema_version: SEGMENT_POOL_GENESIS_VERSION,
            pool_id,
            creator_database_identity: database_identity.to_string(),
            legacy_source,
            auth_tag: [0; 32],
        };
        let genesis = SegmentPoolGenesis {
            auth_tag: genesis_tag(
                &auth_key,
                genesis.schema_version,
                genesis.pool_id,
                &genesis.creator_database_identity,
                genesis.legacy_source.as_ref(),
            ),
            ..genesis
        };
        let create = object_store
            .put_opts(
                &Path::from(SEGMENT_POOL_GENESIS_PATH),
                serde_json::to_vec(&genesis)
                    .map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?
                    .into(),
                PutOptions::from(PutMode::Create),
            )
            .await;
        if create.is_ok() {
            return Ok(SegmentPoolAuthority {
                pool_id,
                auth_key,
                legacy_source: genesis.legacy_source,
            });
        }
        if let Some(authority) = load_pool_authority(&object_store, auth_key).await? {
            return Ok(authority);
        }
        Err(SegmentStoreError::ObjectStore(format!(
            "failed to establish shared segment-pool genesis: {}",
            create.expect_err("successful create returned above")
        )))
    }

    /// Prove that every physical segment already present in this pool belongs
    /// to an epoch that was reserved before counter zero was written. This
    /// deliberately rejects manually copied legacy segments: supporting those
    /// requires a separately reviewed migration manifest or an ID rewrite.
    pub async fn validate_epoch_reservations(
        object_store: Arc<dyn ObjectStore>,
        authority: &SegmentPoolAuthority,
    ) -> Result<()> {
        let mut segments = object_store.list(Some(&Path::from("segments")));
        while let Some(object) = segments.next().await {
            let object = object.map_err(|error| {
                SegmentStoreError::ObjectStore(format!(
                    "failed to inventory shared segment pool: {error}"
                ))
            })?;
            let key = object.location.to_string();
            let segid = Segid::from_object_key(&key).filter(|segid| segid.object_key() == key);
            let segid = segid.ok_or_else(|| {
                SegmentStoreError::ObjectStore(format!(
                    "shared segment pool contains a noncanonical segment key: {key}"
                ))
            })?;
            let marker_path =
                Path::from(format!("{SEGMENT_EPOCH_PREFIX}/{:016x}.json", segid.epoch));
            let marker = object_store
                .get(&marker_path)
                .await
                .map_err(|error| {
                    SegmentStoreError::ObjectStore(format!(
                        "shared segment epoch {} has no readable permanent reservation: {error}",
                        segid.epoch
                    ))
                })?
                .bytes()
                .await
                .map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?;
            let marker: SegmentEpochReservation =
                serde_json::from_slice(&marker).map_err(|error| {
                    SegmentStoreError::ObjectStore(format!(
                        "shared segment epoch {} has an invalid reservation marker: {error}",
                        segid.epoch
                    ))
                })?;
            if !matches!(
                marker.schema_version,
                LEGACY_SEGMENT_EPOCH_RESERVATION_VERSION | SEGMENT_EPOCH_RESERVATION_VERSION
            ) || marker.pool_id != authority.pool_id
                || marker.epoch != segid.epoch
                || marker.reservation_id.is_nil()
                || marker.branch_id.is_some_and(|id| id.is_nil())
                || !reservation_tag_is_valid(authority, &marker)
            {
                return Err(SegmentStoreError::ObjectStore(format!(
                    "shared segment epoch {} has a mismatched reservation marker",
                    segid.epoch
                )));
            }
        }
        Ok(())
    }

    /// Import one offline legacy database's per-database segments without
    /// changing any `Segid`/`FrameLoc`. The first attempt rejects every target
    /// collision before publishing an authenticated intent. Retries reconcile
    /// only objects protected by exact per-segment claims.
    pub async fn migrate_legacy_segments(
        object_store: Arc<dyn ObjectStore>,
        authority: &SegmentPoolAuthority,
        database_path: &Path,
        segment_pool_path: &Path,
        database_instance_id: uuid::Uuid,
        wrapped_key_digest: [u8; 32],
    ) -> Result<LegacyMigrationReport> {
        let database_identity = database_path.to_string();
        if database_identity.is_empty() || database_identity.len() > 4 * 1024 {
            return Err(SegmentStoreError::ObjectStore(
                "legacy migration database identity must contain 1..=4096 bytes".to_string(),
            ));
        }
        let pool_store: Arc<dyn ObjectStore> =
            Arc::new(slatedb::object_store::prefix::PrefixStore::new(
                Arc::clone(&object_store),
                segment_pool_path.clone(),
            ));
        if database_instance_id.is_nil() {
            return Err(SegmentStoreError::ObjectStore(
                "legacy database instance identity must be non-nil".to_string(),
            ));
        }
        let migration_id = legacy_migration_id(authority, &database_identity);
        Self::legacy_migration_required(
            &pool_store,
            authority,
            &database_identity,
            database_instance_id,
        )
        .await?;
        if let Some(report) = Self::legacy_migration_completion(
            &pool_store,
            authority,
            &database_identity,
            database_instance_id,
            Some(wrapped_key_digest),
        )
        .await?
        {
            return Ok(report);
        }

        let intent_path = legacy_intent_path(migration_id);
        let existing_intent = load_json::<LegacyMigrationIntent>(&pool_store, &intent_path).await?;
        let intent = if let Some(intent) = existing_intent {
            intent
        } else {
            // A first attempt must prove the target has no duplicate physical
            // identity and durably bind the complete initial source inventory
            // before it creates any migration-owned object.
            let mut initial = LegacyInventory::default();
            let source_prefix = database_path.clone().join("segments");
            let mut source = object_store.list(Some(&source_prefix));
            while let Some(object) = source.next().await {
                let object = object.map_err(|error| {
                    SegmentStoreError::ObjectStore(format!(
                        "failed to inventory legacy segments: {error}"
                    ))
                })?;
                let (relative, segid) =
                    legacy_relative_segment_key(database_path, &object.location)?;
                let target = path_below(segment_pool_path, &relative);
                match object_store.head(&target).await {
                    Err(slatedb::object_store::Error::NotFound { .. }) => {}
                    Ok(_) => {
                        return Err(SegmentStoreError::ObjectStore(format!(
                            "legacy migration rejects duplicate physical segment {relative}"
                        )));
                    }
                    Err(error) => {
                        return Err(SegmentStoreError::ObjectStore(format!(
                            "failed to inspect migration target {target}: {error}"
                        )));
                    }
                }
                verify_segment_key_matches_footer(&object_store, &object.location, segid).await?;
                let (size, content_digest) = digest_object(&object_store, &object.location).await?;
                initial.include(segid, size, content_digest)?;
            }
            let mut intent = LegacyMigrationIntent {
                schema_version: LEGACY_MIGRATION_VERSION,
                pool_id: authority.pool_id,
                migration_id,
                database_identity: database_identity.clone(),
                database_instance_id,
                wrapped_key_digest,
                initial_segment_count: initial.segment_count,
                initial_total_bytes: initial.total_bytes,
                initial_inventory_fingerprint: initial.fingerprint,
                auth_tag: [0; 32],
            };
            intent.auth_tag = legacy_intent_tag(authority, &intent);
            intent
        };
        create_json_exact(&pool_store, &intent_path, &intent).await?;

        let source_prefix = database_path.clone().join("segments");
        let mut source = object_store.list(Some(&source_prefix));
        while let Some(object) = source.next().await {
            let object = object.map_err(|error| {
                SegmentStoreError::ObjectStore(format!(
                    "failed to inventory legacy segments: {error}"
                ))
            })?;
            let (relative, segid) = legacy_relative_segment_key(database_path, &object.location)?;
            Self::reserve_imported_epoch(Arc::clone(&pool_store), authority, segid.epoch).await?;
            verify_segment_key_matches_footer(&object_store, &object.location, segid).await?;
            let (size, content_digest) = digest_object(&object_store, &object.location).await?;
            let mut claim = LegacySegmentClaim {
                schema_version: LEGACY_MIGRATION_VERSION,
                pool_id: authority.pool_id,
                migration_id,
                database_identity: database_identity.clone(),
                database_instance_id,
                wrapped_key_digest,
                segid,
                size,
                content_digest,
                auth_tag: [0; 32],
            };
            claim.auth_tag = legacy_claim_tag(authority, &claim);
            let claim_path = legacy_claim_path(segid);
            let claim_existed = load_json::<LegacySegmentClaim>(&pool_store, &claim_path)
                .await?
                .is_some();
            if !claim_existed {
                let target = path_below(segment_pool_path, &relative);
                match object_store.head(&target).await {
                    Err(slatedb::object_store::Error::NotFound { .. }) => {}
                    Ok(_) => {
                        return Err(SegmentStoreError::ObjectStore(format!(
                            "legacy migration rejects unclaimed physical segment {relative}"
                        )));
                    }
                    Err(error) => {
                        return Err(SegmentStoreError::ObjectStore(error.to_string()));
                    }
                }
            }
            let claim_created = create_json_exact(&pool_store, &claim_path, &claim).await?;
            let target = path_below(segment_pool_path, &relative);
            match object_store
                .copy_opts(
                    &object.location,
                    &target,
                    CopyOptions {
                        mode: CopyMode::Create,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(()) => {}
                Err(slatedb::object_store::Error::AlreadyExists { .. }) if !claim_created => {}
                Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
                    return Err(SegmentStoreError::ObjectStore(format!(
                        "physical segment {relative} appeared after its migration preflight"
                    )));
                }
                Err(error) => return Err(SegmentStoreError::ObjectStore(error.to_string())),
            }
            let target_identity = digest_object(&object_store, &target).await?;
            if target_identity != (size, content_digest) {
                return Err(SegmentStoreError::ObjectStore(format!(
                    "copied physical segment {relative} does not match its immutable source"
                )));
            }
        }

        // A second full pass catches source additions, removals, or changes and
        // authenticates every target against its immutable claim before the
        // completion record makes startup legal.
        let mut verified = LegacyInventory::default();
        let source_prefix = database_path.clone().join("segments");
        let mut source = object_store.list(Some(&source_prefix));
        while let Some(object) = source.next().await {
            let object = object.map_err(|error| {
                SegmentStoreError::ObjectStore(format!(
                    "failed to verify legacy inventory: {error}"
                ))
            })?;
            let (relative, segid) = legacy_relative_segment_key(database_path, &object.location)?;
            verify_segment_key_matches_footer(&object_store, &object.location, segid).await?;
            let (size, content_digest) = digest_object(&object_store, &object.location).await?;
            let claim = load_json::<LegacySegmentClaim>(&pool_store, &legacy_claim_path(segid))
                .await?
                .ok_or_else(|| {
                    SegmentStoreError::ObjectStore(format!(
                        "legacy segment {relative} has no immutable import claim"
                    ))
                })?;
            if claim.schema_version != LEGACY_MIGRATION_VERSION
                || claim.pool_id != authority.pool_id
                || claim.migration_id != migration_id
                || claim.database_identity != database_identity
                || claim.database_instance_id != database_instance_id
                || claim.wrapped_key_digest != wrapped_key_digest
                || claim.segid != segid
                || claim.size != size
                || claim.content_digest != content_digest
                || claim.auth_tag != legacy_claim_tag(authority, &claim)
            {
                return Err(SegmentStoreError::ObjectStore(format!(
                    "legacy segment {relative} has a conflicting or unauthenticated claim"
                )));
            }
            let target = path_below(segment_pool_path, &relative);
            if digest_object(&object_store, &target).await? != (size, content_digest) {
                return Err(SegmentStoreError::ObjectStore(format!(
                    "migration target {relative} changed before completion"
                )));
            }
            verified.include(segid, size, content_digest)?;
        }
        // The durable pool-global claim set is the inventory boundary across
        // crashes. Enumerating it prevents a retry from forgetting a segment
        // which disappeared from the source after its claim was published.
        let mut claimed = LegacyInventory::default();
        let mut claims = pool_store.list(Some(&Path::from(LEGACY_SEGMENT_CLAIM_PREFIX)));
        while let Some(object) = claims.next().await {
            let object = object.map_err(|error| {
                SegmentStoreError::ObjectStore(format!(
                    "failed to inventory durable legacy claims: {error}"
                ))
            })?;
            let claim = load_json::<LegacySegmentClaim>(&pool_store, &object.location)
                .await?
                .ok_or_else(|| {
                    SegmentStoreError::ObjectStore(format!(
                        "legacy claim {} disappeared during verification",
                        object.location
                    ))
                })?;
            if claim.schema_version != LEGACY_MIGRATION_VERSION
                || claim.pool_id != authority.pool_id
                || claim.database_instance_id.is_nil()
                || claim.auth_tag != legacy_claim_tag(authority, &claim)
                || legacy_claim_path(claim.segid) != object.location
            {
                return Err(SegmentStoreError::ObjectStore(format!(
                    "pool contains a corrupt or noncanonical legacy claim: {}",
                    object.location
                )));
            }
            if claim.migration_id != migration_id {
                continue;
            }
            if claim.database_identity != database_identity
                || claim.database_instance_id != database_instance_id
                || claim.wrapped_key_digest != wrapped_key_digest
            {
                return Err(SegmentStoreError::ObjectStore(format!(
                    "migration claim {} is bound to another legacy incarnation",
                    object.location
                )));
            }
            let target = path_below(segment_pool_path, &claim.segid.object_key());
            if digest_object(&object_store, &target).await? != (claim.size, claim.content_digest) {
                return Err(SegmentStoreError::ObjectStore(format!(
                    "claimed migration target {} is absent or changed",
                    claim.segid.object_key()
                )));
            }
            claimed.include(claim.segid, claim.size, claim.content_digest)?;
        }
        if claimed.segment_count != verified.segment_count
            || claimed.total_bytes != verified.total_bytes
            || claimed.fingerprint != verified.fingerprint
            || claimed.segment_count != intent.initial_segment_count
            || claimed.total_bytes != intent.initial_total_bytes
            || claimed.fingerprint != intent.initial_inventory_fingerprint
        {
            return Err(SegmentStoreError::ObjectStore(
                "legacy segment inventory differs from its durable migration intent or claims"
                    .to_string(),
            ));
        }
        let mut completion = LegacyMigrationCompletion {
            schema_version: LEGACY_MIGRATION_VERSION,
            pool_id: authority.pool_id,
            migration_id,
            database_identity: database_identity.clone(),
            database_instance_id,
            wrapped_key_digest,
            segment_count: claimed.segment_count,
            total_bytes: claimed.total_bytes,
            inventory_fingerprint: claimed.fingerprint,
            auth_tag: [0; 32],
        };
        completion.auth_tag = legacy_completion_tag(authority, &completion);
        create_json_exact(
            &pool_store,
            &legacy_completion_path(migration_id),
            &completion,
        )
        .await?;
        Ok(LegacyMigrationReport {
            migration_id,
            segment_count: completion.segment_count,
            total_bytes: completion.total_bytes,
            inventory_fingerprint: completion.inventory_fingerprint,
        })
    }

    /// Authenticate the immutable completion record for one legacy database.
    /// Startup uses this before allowing old per-database segment objects to
    /// coexist with the configured shared pool.
    pub async fn legacy_migration_required(
        pool_store: &Arc<dyn ObjectStore>,
        authority: &SegmentPoolAuthority,
        database_identity: &str,
        database_instance_id: uuid::Uuid,
    ) -> Result<bool> {
        if database_instance_id.is_nil() {
            return Err(SegmentStoreError::ObjectStore(
                "legacy database instance identity must be non-nil".to_string(),
            ));
        }
        let mut required = false;
        let mut bootstraps = pool_store.list(Some(&Path::from(LEGACY_MIGRATION_BOOTSTRAP_PREFIX)));
        while let Some(object) = bootstraps.next().await {
            let object =
                object.map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?;
            let bootstrap = load_json::<LegacyMigrationBootstrap>(pool_store, &object.location)
                .await?
                .ok_or_else(|| {
                    SegmentStoreError::ObjectStore(format!(
                        "legacy migration bootstrap {} disappeared",
                        object.location
                    ))
                })?;
            if bootstrap.schema_version != LEGACY_MIGRATION_VERSION
                || bootstrap.database_instance_id.is_nil()
                || legacy_bootstrap_path(bootstrap.database_instance_id) != object.location
            {
                return Err(SegmentStoreError::ObjectStore(format!(
                    "pool contains a corrupt legacy migration bootstrap: {}",
                    object.location
                )));
            }
            if bootstrap.database_identity == database_identity {
                if bootstrap.database_instance_id != database_instance_id {
                    return Err(SegmentStoreError::ObjectStore(
                        "legacy migration bootstrap is bound to a different database incarnation"
                            .to_string(),
                    ));
                }
                required = true;
            }
        }
        if let Some(source) = authority.legacy_source.as_ref() {
            if source.database_identity == database_identity {
                if source.database_instance_id != database_instance_id {
                    return Err(SegmentStoreError::ObjectStore(
                        "shared-pool genesis is bound to a different legacy database incarnation"
                            .to_string(),
                    ));
                }
                required = true;
            } else if source.database_instance_id == database_instance_id {
                return Err(SegmentStoreError::ObjectStore(
                    "legacy database incarnation moved after shared-pool genesis".to_string(),
                ));
            }
        }
        let migration_id = legacy_migration_id(authority, database_identity);
        if let Some(intent) =
            load_json::<LegacyMigrationIntent>(pool_store, &legacy_intent_path(migration_id))
                .await?
        {
            if intent.schema_version != LEGACY_MIGRATION_VERSION
                || intent.pool_id != authority.pool_id
                || intent.migration_id != migration_id
                || intent.database_identity != database_identity
                || intent.database_instance_id != database_instance_id
                || intent.auth_tag != legacy_intent_tag(authority, &intent)
            {
                return Err(SegmentStoreError::ObjectStore(
                    "legacy migration intent is unauthenticated or conflicts with this database"
                        .to_string(),
                ));
            }
            if authority.legacy_source.as_ref().is_some_and(|genesis| {
                genesis.database_identity == database_identity
                    && (genesis.database_instance_id != intent.database_instance_id
                        || genesis.wrapped_key_digest != intent.wrapped_key_digest)
            }) {
                return Err(SegmentStoreError::ObjectStore(
                    "legacy migration intent conflicts with the source bound into pool genesis"
                        .to_string(),
                ));
            }
            required = true;
        }
        Ok(required)
    }

    pub async fn legacy_migration_completion(
        pool_store: &Arc<dyn ObjectStore>,
        authority: &SegmentPoolAuthority,
        database_identity: &str,
        database_instance_id: uuid::Uuid,
        expected_wrapped_key_digest: Option<[u8; 32]>,
    ) -> Result<Option<LegacyMigrationReport>> {
        let migration_id = legacy_migration_id(authority, database_identity);
        let Some(completion) = load_json::<LegacyMigrationCompletion>(
            pool_store,
            &legacy_completion_path(migration_id),
        )
        .await?
        else {
            return Ok(None);
        };
        let intent =
            load_json::<LegacyMigrationIntent>(pool_store, &legacy_intent_path(migration_id))
                .await?
                .ok_or_else(|| {
                    SegmentStoreError::ObjectStore(
                        "legacy migration completion has no immutable intent".to_string(),
                    )
                })?;
        if completion.schema_version != LEGACY_MIGRATION_VERSION
            || completion.pool_id != authority.pool_id
            || completion.migration_id != migration_id
            || completion.database_identity != database_identity
            || completion.database_instance_id != database_instance_id
            || expected_wrapped_key_digest
                .is_some_and(|digest| digest != completion.wrapped_key_digest)
            || intent.schema_version != LEGACY_MIGRATION_VERSION
            || intent.pool_id != authority.pool_id
            || intent.migration_id != migration_id
            || intent.database_identity != completion.database_identity
            || intent.database_instance_id != completion.database_instance_id
            || intent.wrapped_key_digest != completion.wrapped_key_digest
            || intent.initial_segment_count != completion.segment_count
            || intent.initial_total_bytes != completion.total_bytes
            || intent.initial_inventory_fingerprint != completion.inventory_fingerprint
            || intent.auth_tag != legacy_intent_tag(authority, &intent)
            || completion.auth_tag != legacy_completion_tag(authority, &completion)
        {
            return Err(SegmentStoreError::ObjectStore(
                "legacy migration completion is unauthenticated or conflicts with this database"
                    .to_string(),
            ));
        }
        Ok(Some(LegacyMigrationReport {
            migration_id,
            segment_count: completion.segment_count,
            total_bytes: completion.total_bytes,
            inventory_fingerprint: completion.inventory_fingerprint,
        }))
    }

    async fn reserve_imported_epoch(
        pool_store: Arc<dyn ObjectStore>,
        authority: &SegmentPoolAuthority,
        epoch: u64,
    ) -> Result<()> {
        if epoch == 0 {
            return Err(SegmentStoreError::ObjectStore(
                "legacy segment epoch zero cannot be imported".to_string(),
            ));
        }
        let reservation_owner = format!("legacy-import/{}/{epoch:016x}", authority.pool_id);
        let digest = authentication_tag(
            &authority.auth_key,
            &[
                b"legacy-imported-epoch-id",
                authority.pool_id.as_bytes(),
                &epoch.to_be_bytes(),
            ],
        );
        let mut id_bytes: [u8; 16] = digest[..16].try_into().expect("digest prefix is 16 bytes");
        id_bytes[6] = (id_bytes[6] & 0x0f) | 0x40;
        id_bytes[8] = (id_bytes[8] & 0x3f) | 0x80;
        let reservation_id = uuid::Uuid::from_bytes(id_bytes);
        let marker = SegmentEpochReservation {
            schema_version: SEGMENT_EPOCH_RESERVATION_VERSION,
            pool_id: authority.pool_id,
            epoch,
            reservation_id,
            database_identity: reservation_owner.clone(),
            branch_id: None,
            auth_tag: reservation_tag(authority, epoch, reservation_id, &reservation_owner, None),
        };
        let path = Path::from(format!("{SEGMENT_EPOCH_PREFIX}/{epoch:016x}.json"));
        create_json_exact(&pool_store, &path, &marker).await?;
        Ok(())
    }

    /// Reserve a never-reused writer epoch in the exact segment pool using an
    /// immutable conditional-create marker. All writers sharing the pool use
    /// this before allocating counter-zero, so shallow clones cannot collide
    /// merely because their independent SlateDB manifests reuse an epoch.
    pub async fn reserve_epoch(
        object_store: Arc<dyn ObjectStore>,
        authority: &SegmentPoolAuthority,
        database_identity: &str,
    ) -> Result<u64> {
        Self::reserve_epoch_for_branch(object_store, authority, database_identity, None).await
    }

    /// Reserve an authenticated epoch owned by one exact branch incarnation.
    /// This is necessary, but intentionally not sufficient, for private local
    /// GC; authoritative catalog sealing/exposure state is required as well.
    #[allow(dead_code)] // wired only after the authoritative epoch lifecycle lands
    pub async fn reserve_branch_epoch(
        object_store: Arc<dyn ObjectStore>,
        authority: &SegmentPoolAuthority,
        database_identity: &str,
        branch_id: uuid::Uuid,
    ) -> Result<u64> {
        if branch_id.is_nil() {
            return Err(SegmentStoreError::ObjectStore(
                "segment epoch branch identity must be non-nil".to_string(),
            ));
        }
        Self::reserve_epoch_for_branch(object_store, authority, database_identity, Some(branch_id))
            .await
    }

    /// Authenticate one exact v2 branch-bound reservation directly from its
    /// immutable pool marker. Legacy and ownerless markers remain valid for
    /// global uniqueness, but can never produce private-ownership evidence.
    #[allow(dead_code)] // The standalone binary does not yet open the branch catalog lifecycle.
    pub async fn authenticate_branch_epoch(
        object_store: Arc<dyn ObjectStore>,
        authority: &SegmentPoolAuthority,
        epoch: u64,
    ) -> Result<AuthenticatedBranchEpoch> {
        if epoch == 0 {
            return Err(SegmentStoreError::ObjectStore(
                "branch-owned segment epoch must be nonzero".to_string(),
            ));
        }
        let marker_path = Path::from(format!("{SEGMENT_EPOCH_PREFIX}/{epoch:016x}.json"));
        let bytes = object_store
            .get(&marker_path)
            .await
            .map_err(|error| {
                SegmentStoreError::ObjectStore(format!(
                    "branch-owned segment epoch {epoch} has no readable reservation: {error}"
                ))
            })?
            .bytes()
            .await
            .map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?;
        let marker: SegmentEpochReservation = serde_json::from_slice(&bytes).map_err(|error| {
            SegmentStoreError::ObjectStore(format!(
                "branch-owned segment epoch {epoch} has an invalid reservation marker: {error}"
            ))
        })?;
        let branch_id = marker.branch_id.ok_or_else(|| {
            SegmentStoreError::ObjectStore(format!(
                "segment epoch {epoch} is global-only and has no authenticated branch owner"
            ))
        })?;
        if marker.schema_version != SEGMENT_EPOCH_RESERVATION_VERSION
            || marker.pool_id != authority.pool_id
            || marker.epoch != epoch
            || marker.reservation_id.is_nil()
            || branch_id.is_nil()
            || marker.database_identity.is_empty()
            || marker.database_identity.len() > 4 * 1024
            || !reservation_tag_is_valid(authority, &marker)
        {
            return Err(SegmentStoreError::ObjectStore(format!(
                "branch-owned segment epoch {epoch} has a mismatched reservation marker"
            )));
        }
        Ok(AuthenticatedBranchEpoch {
            pool_id: marker.pool_id,
            epoch: marker.epoch,
            reservation_id: marker.reservation_id,
            database_identity: marker.database_identity,
            branch_id,
        })
    }

    async fn reserve_epoch_for_branch(
        object_store: Arc<dyn ObjectStore>,
        authority: &SegmentPoolAuthority,
        database_identity: &str,
        branch_id: Option<uuid::Uuid>,
    ) -> Result<u64> {
        if database_identity.len() > 4 * 1024 {
            return Err(SegmentStoreError::ObjectStore(
                "segment epoch database identity must contain at most 4096 bytes".to_string(),
            ));
        }
        for _ in 0..64 {
            let reservation_id = uuid::Uuid::new_v4();
            let epoch = u64::from_be_bytes(
                reservation_id.as_bytes()[..8]
                    .try_into()
                    .expect("UUID prefix is eight bytes"),
            );
            if epoch == 0 {
                continue;
            }
            let marker = SegmentEpochReservation {
                schema_version: SEGMENT_EPOCH_RESERVATION_VERSION,
                pool_id: authority.pool_id,
                epoch,
                reservation_id,
                database_identity: database_identity.to_string(),
                branch_id,
                auth_tag: reservation_tag(
                    authority,
                    epoch,
                    reservation_id,
                    database_identity,
                    branch_id,
                ),
            };
            let path = Path::from(format!("{SEGMENT_EPOCH_PREFIX}/{epoch:016x}.json"));
            match object_store
                .put_opts(
                    &path,
                    serde_json::to_vec(&marker)
                        .map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?
                        .into(),
                    PutOptions::from(PutMode::Create),
                )
                .await
            {
                Ok(_) => return Ok(epoch),
                Err(slatedb::object_store::Error::AlreadyExists { .. }) => continue,
                Err(error) => {
                    return Err(SegmentStoreError::ObjectStore(error.to_string()));
                }
            }
        }
        Err(SegmentStoreError::ObjectStore(
            "failed to reserve a globally unique segment epoch after 64 attempts".to_string(),
        ))
    }

    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        codec: FrameCodec,
        epoch: u64,
        warm: Option<SegmentWarmHook>,
    ) -> Self {
        Self {
            object_store,
            codec: Arc::new(codec),
            epoch,
            counter: AtomicU64::new(0),
            read_calls: AtomicU64::new(0),
            warm,
        }
    }

    /// A shared handle to the frame codec (for the open-segment buffer).
    pub fn codec(&self) -> Arc<FrameCodec> {
        Arc::clone(&self.codec)
    }

    /// Allocate the next epoch-namespaced segment id.
    pub fn next_segid(&self) -> Segid {
        Segid::new(self.epoch, self.counter.fetch_add(1, Ordering::Relaxed))
    }

    /// PUT pre-built segment bytes (durable on return). Used by the open-segment
    /// buffer's seal, which builds the bytes itself via `seal_directory` +
    /// `assemble_segment`.
    pub async fn put_segment(&self, segid: Segid, bytes: Bytes) -> Result<()> {
        // A small (partial-fsync) seal goes up as a single PUT (which still
        // writes through the parts cache) but a full 256 MiB seal streams as
        // concurrent multipart, so the fsync-path PUT latency stays bounded
        // instead of serializing 256 MiB on one stream.
        let path = Path::from(segid.object_key());
        if bytes.len() < SEAL_PART_SIZE {
            self.put_segment_create(&path, &bytes).await?;
        } else {
            self.put_segment_multipart(&path, &bytes).await?;
        }
        // The multipart path doesn't write through the parts cache; warm it
        // with the bytes in hand, after the upload commits (mirroring the
        // single-PUT path).
        if let Some(warm) = &self.warm {
            warm(&path, bytes);
        }
        Ok(())
    }

    /// Atomically create an immutable segment key. A retried request whose
    /// success response was lost is accepted only when the existing bytes are
    /// exactly the bytes this writer intended to publish.
    async fn put_segment_create(&self, path: &Path, bytes: &Bytes) -> Result<()> {
        match self
            .object_store
            .put_opts(
                path,
                bytes.clone().into(),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => self.reconcile_segment_create(path, bytes, error).await,
        }
    }

    #[allow(dead_code)] // Used by the disabled private-epoch collector coordinator.
    pub(crate) async fn put_private_gc_artifact(
        &self,
        guard_id: uuid::Uuid,
        bytes: &Bytes,
    ) -> Result<()> {
        let path = Path::from(format!("{PRIVATE_GC_ARTIFACT_PREFIX}/{guard_id}.bin"));
        self.put_segment_create(&path, bytes).await
    }

    /// Read one immutable private-GC artifact without permitting an untrusted
    /// object to allocate beyond the caller's format bound.
    #[allow(dead_code)] // Used by the disabled private-epoch collector coordinator.
    pub(crate) async fn get_private_gc_artifact(
        &self,
        guard_id: uuid::Uuid,
        max_bytes: u64,
    ) -> Result<Bytes> {
        let path = Path::from(format!("{PRIVATE_GC_ARTIFACT_PREFIX}/{guard_id}.bin"));
        let result = self
            .object_store
            .get(&path)
            .await
            .map_err(|error| match error {
                slatedb::object_store::Error::NotFound { .. } => SegmentStoreError::NotFound,
                other => SegmentStoreError::ObjectStore(other.to_string()),
            })?;
        let expected_size = result.meta.size;
        if expected_size > max_bytes {
            return Err(SegmentStoreError::ObjectStore(format!(
                "private GC artifact {guard_id} exceeds its format bound"
            )));
        }
        let capacity = usize::try_from(expected_size).map_err(|_| {
            SegmentStoreError::ObjectStore(format!(
                "private GC artifact {guard_id} length cannot fit in memory"
            ))
        })?;
        let mut bytes = BytesMut::with_capacity(capacity);
        let mut stream = result.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?;
            let next_len = bytes.len().checked_add(chunk.len()).ok_or_else(|| {
                SegmentStoreError::ObjectStore(format!(
                    "private GC artifact {guard_id} length overflow"
                ))
            })?;
            if next_len as u64 > max_bytes {
                return Err(SegmentStoreError::ObjectStore(format!(
                    "private GC artifact {guard_id} body exceeds its format bound"
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() as u64 != expected_size {
            return Err(SegmentStoreError::ObjectStore(format!(
                "private GC artifact {guard_id} body length disagrees with object metadata"
            )));
        }
        Ok(bytes.freeze())
    }

    async fn reconcile_segment_create(
        &self,
        path: &Path,
        bytes: &Bytes,
        create_error: slatedb::object_store::Error,
    ) -> Result<()> {
        match self.existing_segment_matches(path, bytes).await {
            Ok(true) => Ok(()),
            Ok(false)
                if matches!(
                    &create_error,
                    slatedb::object_store::Error::AlreadyExists { .. }
                ) =>
            {
                Err(SegmentStoreError::ObjectStore(format!(
                    "immutable segment key {path} already contains different bytes"
                )))
            }
            Ok(false) | Err(_) => Err(SegmentStoreError::ObjectStore(create_error.to_string())),
        }
    }

    /// Compare an existing object without buffering another segment-sized
    /// allocation. `false` means either absent or byte-different.
    async fn existing_segment_matches(&self, path: &Path, expected: &Bytes) -> Result<bool> {
        let result = match self.object_store.get(path).await {
            Ok(result) => result,
            Err(slatedb::object_store::Error::NotFound { .. }) => return Ok(false),
            Err(error) => return Err(SegmentStoreError::ObjectStore(error.to_string())),
        };
        if result.meta.size != expected.len() as u64 {
            return Ok(false);
        }
        let mut offset = 0usize;
        let mut stream = result.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?;
            let end = offset.checked_add(chunk.len()).ok_or_else(|| {
                SegmentStoreError::ObjectStore(format!(
                    "segment object {path} exceeded addressable size while reconciling create"
                ))
            })?;
            if end > expected.len() || chunk.as_ref() != &expected[offset..end] {
                return Ok(false);
            }
            offset = end;
        }
        Ok(offset == expected.len())
    }

    /// Multipart PUT of `bytes` in `SEAL_PART_SIZE` parts, at most
    /// `SEAL_UPLOAD_CONCURRENCY` in flight. Any failure aborts the upload before
    /// the error propagates: parts of an unfinished multipart upload are
    /// invisible to LIST — and so to the orphan sweep — yet billed until
    /// aborted, and each retried seal targets a fresh upload, so leaks would
    /// accrete per failure.
    async fn put_segment_multipart(&self, path: &Path, bytes: &Bytes) -> Result<()> {
        // Multipart APIs do not expose Create semantics. Upload under a unique
        // non-authoritative temporary key, then atomically copy-if-absent into
        // the immutable segment namespace.
        let upload_path = Path::from(format!("{SEGMENT_UPLOAD_PREFIX}/{}", uuid::Uuid::new_v4()));
        let mut upload = self
            .object_store
            .put_multipart(&upload_path)
            .await
            .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
        // Parts run as spawned tasks, so upload progress never waits on this
        // future being polled.
        let mut parts = tokio::task::JoinSet::new();
        let uploaded = async {
            let mut rest = bytes.clone();
            while !rest.is_empty() {
                while parts.len() >= SEAL_UPLOAD_CONCURRENCY {
                    parts
                        .join_next()
                        .await
                        .expect("parts is non-empty")
                        .expect("part upload panicked")?;
                }
                let part = rest.split_to(rest.len().min(SEAL_PART_SIZE));
                parts.spawn(upload.put_part(part.into()));
            }
            while let Some(part) = parts.join_next().await {
                part.expect("part upload panicked")?;
            }
            upload.complete().await
        }
        .await;
        if let Err(e) = uploaded {
            // Best-effort cleanup: surface the seal error even if the abort
            // itself fails (leaving the parts to the backend's lifecycle rule).
            parts.shutdown().await;
            if let Err(abort_err) = upload.abort().await {
                tracing::warn!(
                    "segment seal: aborting failed upload of {upload_path}: {abort_err}"
                );
            }
            return Err(SegmentStoreError::ObjectStore(e.to_string()));
        }
        let copy_result = self
            .object_store
            .copy_opts(
                &upload_path,
                path,
                CopyOptions {
                    mode: CopyMode::Create,
                    ..Default::default()
                },
            )
            .await;
        let result = match copy_result {
            Ok(()) => Ok(()),
            Err(error) => self.reconcile_segment_create(path, bytes, error).await,
        };
        if let Err(error) = self.object_store.delete(&upload_path).await {
            tracing::warn!("segment seal: deleting temporary upload {upload_path}: {error}");
        }
        result
    }

    /// Seal `frames` (each `(inode, extent, full-extent plaintext)`) into one new
    /// segment object, durable on return. Returns each frame's location.
    #[cfg(test)] // production packs via seal_compressed; tests build plaintext worlds here
    pub async fn seal(
        &self,
        frames: &[(InodeId, u64, Bytes)],
    ) -> Result<Vec<(InodeId, u64, FrameLoc)>> {
        let compressed = frames
            .iter()
            .map(|(id, extent, data)| {
                let payload = self.codec.compress(data).map_err(SegmentError::from)?;
                Ok((*id, *extent, payload))
            })
            .collect::<Result<Vec<_>>>()?;
        self.seal_compressed(compressed).await
    }

    /// As [`Self::seal`] for already-compressed payloads (relocated frames read
    /// via [`Self::read_compressed_run`]): each re-seals under its new slot's
    /// AAD, never decompressed, so compaction's memory tracks stored size. A
    /// large batch's seals fan out on rayon ([`seal_compressed_batch`]); the
    /// appends assign offsets in the same order.
    pub async fn seal_compressed(
        &self,
        frames: Vec<(InodeId, u64, Compressed)>,
    ) -> Result<Vec<(InodeId, u64, FrameLoc)>> {
        let segid = self.next_segid();
        let sealed = seal_compressed_batch(&self.codec, segid, frames)?;
        let mut builder = SegmentBuilder::new(&self.codec, segid);
        let mut locs = Vec::with_capacity(sealed.len());
        for (id, extent, body) in sealed {
            let byte_offset = builder.byte_len();
            let frame_index = builder.append_sealed(id, extent, &body);
            let byte_len = (builder.byte_len() - byte_offset) as u32;
            locs.push((
                id,
                extent,
                FrameLoc {
                    segid,
                    frame_index,
                    byte_offset,
                    byte_len,
                },
            ));
        }
        let bytes = builder.finish(segid.counter)?;
        self.put_segment(segid, Bytes::from(bytes)).await?;
        Ok(locs)
    }

    /// Read one extent's plaintext via a ranged GET of just its frame.
    pub async fn read_extent(&self, loc: FrameLoc, id: InodeId, extent: u64) -> Result<Bytes> {
        let mut frames = self
            .read_run(
                loc.segid,
                loc.byte_offset,
                loc.byte_len,
                loc.frame_index,
                &[(id, extent)],
            )
            .await?;
        Ok(frames.pop().expect("one frame"))
    }

    /// Read a contiguous run of `slots.len()` frames from `segid` in one ranged
    /// GET over `[byte_offset, byte_offset + byte_len)`, returning each plaintext.
    /// `slots[i]` is the `(inode, extent)` of the frame at `first_frame + i`.
    pub async fn read_run(
        &self,
        segid: Segid,
        byte_offset: u64,
        byte_len: u32,
        first_frame: u32,
        slots: &[(InodeId, u64)],
    ) -> Result<Vec<Bytes>> {
        let region = self.read_run_region(segid, byte_offset, byte_len).await?;
        let frames = crate::segment::read_frames_from_region(
            &self.codec,
            &region,
            segid,
            first_frame,
            slots,
        )?;
        Ok(frames.into_iter().map(Bytes::from).collect())
    }

    /// As [`Self::read_run`] but AEAD-verify only, returning still-compressed
    /// payloads for relocation (see [`Self::seal_compressed`]).
    pub async fn read_compressed_run(
        &self,
        segid: Segid,
        byte_offset: u64,
        byte_len: u32,
        first_frame: u32,
        slots: &[(InodeId, u64)],
    ) -> Result<Vec<Compressed>> {
        let region = self.read_run_region(segid, byte_offset, byte_len).await?;
        Ok(crate::segment::read_compressed_frames_from_region(
            &self.codec,
            &region,
            segid,
            first_frame,
            slots,
        )?)
    }

    /// One ranged GET of a frame run's bytes; the parts cache (warmed at seal
    /// time) absorbs the re-read and read-after-write cases.
    async fn read_run_region(
        &self,
        segid: Segid,
        byte_offset: u64,
        byte_len: u32,
    ) -> Result<Bytes> {
        let path = Path::from(segid.object_key());
        let region = self
            .object_store
            .get_range(&path, byte_offset..byte_offset + byte_len as u64)
            .await
            .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        Ok(region)
    }
}

/// GC/maintenance primitives.
impl SegmentStore {
    /// This writer's epoch (segids it produces are namespaced under it).
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Ranged segment GETs issued so far (read-amplification metric).
    #[cfg(test)]
    pub fn read_calls(&self) -> u64 {
        self.read_calls.load(Ordering::Relaxed)
    }

    /// List every `segments/` object currently present. Production reclaim
    /// streams via [`Self::list_segments_stream`]; this collected form serves
    /// the tests and the failpoints harness.
    #[cfg(test)]
    pub async fn list_segments(&self) -> Result<Vec<Segid>> {
        use futures::TryStreamExt;
        self.list_segments_stream()
            .map_ok(|(segid, _, _)| segid)
            .try_collect()
            .await
    }

    /// Stream `(Segid, size, last_modified)` for every segment object, so the GC
    /// can classify on the fly instead of buffering the whole listing
    /// (O(#segments)) in RAM. `last_modified` is the object's creation time
    /// (segments are immutable), used to protect anything that could predate a
    /// persistent checkpoint.
    pub fn list_segments_stream(
        &self,
    ) -> impl futures::Stream<Item = Result<(Segid, u64, chrono::DateTime<chrono::Utc>)>> + '_ {
        // One listing per shard prefix (segments/00 .. segments/ff), flattened
        // with bounded concurrency. Paged LISTs are sequential within a prefix,
        // so per-shard chains parallelize the scan and shrink the unit a
        // retrying layer must buffer per attempt to 1/256th of the keyspace.
        // Shard streams interleave: consumers must not assume key order.
        futures::stream::iter((0..=0xffu8).map(|shard| Path::from(format!("segments/{shard:02x}"))))
            .map(move |prefix| self.object_store.list(Some(&prefix)))
            .flatten_unordered(LIST_SHARD_CONCURRENCY)
            .filter_map(|meta| {
                futures::future::ready(match meta {
                    Ok(m) => Segid::from_object_key(m.location.as_ref())
                        .map(|s| Ok((s, m.size, m.last_modified))),
                    Err(e) => Some(Err(SegmentStoreError::ObjectStore(e.to_string()))),
                })
            })
    }

    /// Delete one segment object.
    pub async fn delete_segment(&self, segid: Segid) -> Result<()> {
        self.object_store
            .delete(&Path::from(segid.object_key()))
            .await
            .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))
    }

    /// Stream the canonical immutable object and bind both its metadata and
    /// bytes. Metadata-only checks are insufficient for irreversible deletion.
    #[allow(dead_code)] // Consumed by the disabled private-epoch collector path.
    pub(crate) async fn object_identity(&self, segid: Segid) -> Result<SegmentObjectIdentity> {
        let result = self
            .object_store
            .get(&Path::from(segid.object_key()))
            .await
            .map_err(|error| match error {
                slatedb::object_store::Error::NotFound { .. } => SegmentStoreError::NotFound,
                other => SegmentStoreError::ObjectStore(other.to_string()),
            })?;
        let meta = result.meta.clone();
        let mut stream = result.into_stream();
        let mut hasher = Sha256::new();
        let mut bytes = 0u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| SegmentStoreError::ObjectStore(error.to_string()))?;
            bytes = bytes.checked_add(chunk.len() as u64).ok_or_else(|| {
                SegmentStoreError::ObjectStore("segment object length overflow".to_string())
            })?;
            hasher.update(&chunk);
        }
        if bytes != meta.size {
            return Err(SegmentStoreError::ObjectStore(format!(
                "segment {segid:?} body length disagrees with object metadata"
            )));
        }
        Ok(SegmentObjectIdentity {
            segid,
            size: meta.size,
            modified_seconds: meta.last_modified.timestamp(),
            modified_nanos: meta.last_modified.timestamp_subsec_nanos(),
            content_digest: hasher.finalize().into(),
        })
    }

    /// Read and decrypt a segment's reverse-map directory (which frame backs
    /// which logical block), for the coalescer.
    pub async fn read_directory(&self, segid: Segid) -> Result<Vec<DirEntry>> {
        let path = Path::from(segid.object_key());
        // Fetch just the footer (last FOOTER_LEN bytes) to locate the directory,
        // then a ranged GET of the directory itself — never the whole object.
        let footer_res = self
            .object_store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Suffix(crate::segment::FOOTER_LEN as u64)),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| match e {
                slatedb::object_store::Error::NotFound { .. } => SegmentStoreError::NotFound,
                other => SegmentStoreError::ObjectStore(other.to_string()),
            })?;
        let object_size = footer_res.meta.size;
        let footer = footer_res
            .bytes()
            .await
            .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
        let meta = crate::segment::parse_footer(&footer, object_size)?;
        // Defense-in-depth: a misdirected read returning a different
        // (self-consistent) segment would feed the wrong directory into the
        // coalescer.
        if meta.segid != segid {
            return Err(crate::segment::SegmentError::SegidMismatch {
                expected: segid,
                found: meta.segid,
            }
            .into());
        }
        let dir_bytes = self
            .object_store
            .get_range(
                &path,
                meta.dir_offset..meta.dir_offset + meta.dir_len as u64,
            )
            .await
            .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
        Ok(crate::segment::decode_directory(
            &self.codec,
            &dir_bytes,
            &footer,
            &meta,
        )?)
    }
}

/// A shipped frame, for the HA standby to rebuild an un-PUT segment on takeover.
pub struct ReconFrame {
    pub frame_index: u32,
    pub byte_offset: u64,
    pub byte_len: u32,
    pub inode: InodeId,
    pub extent: u64,
    pub bytes: Bytes,
}

/// Confirm that an object which won a concurrent create contains every frame the
/// takeover is about to reference. The existing object may be the old leader's
/// full seal (a superset of the replay tail), but it must agree on both the
/// authenticated directory entry and exact sealed bytes of each required frame.
async fn verify_existing_recon_segment(
    object_store: &Arc<dyn ObjectStore>,
    codec: &FrameCodec,
    segid: Segid,
    frames: &[ReconFrame],
) -> Result<()> {
    let path = Path::from(segid.object_key());
    let footer_result = object_store
        .get_opts(
            &path,
            GetOptions {
                range: Some(GetRange::Suffix(FOOTER_LEN as u64)),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
    let object_size = footer_result.meta.size;
    let footer = footer_result
        .bytes()
        .await
        .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
    let meta = crate::segment::parse_footer(&footer, object_size)?;
    if meta.segid != segid {
        return Err(SegmentError::SegidMismatch {
            expected: segid,
            found: meta.segid,
        }
        .into());
    }
    let dir_bytes = object_store
        .get_range(
            &path,
            meta.dir_offset..meta.dir_offset + meta.dir_len as u64,
        )
        .await
        .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
    let dir: HashSet<_> = crate::segment::decode_directory(codec, &dir_bytes, &footer, &meta)?
        .into_iter()
        .map(|entry| (entry.byte_offset, entry.len, entry.inode, entry.extent))
        .collect();

    for frame in frames {
        let body_len = frame
            .byte_len
            .checked_sub(LEN_PREFIX as u32)
            .ok_or_else(|| {
                SegmentStoreError::ObjectStore(format!(
                    "shipped frame is shorter than its length prefix for {segid:?}"
                ))
            })?;
        if !dir.contains(&(frame.byte_offset, body_len, frame.inode, frame.extent)) {
            return Err(SegmentStoreError::ObjectStore(format!(
                "existing segment {segid:?} is missing replayed frame {} for inode {} extent {}",
                frame.frame_index, frame.inode, frame.extent
            )));
        }
    }
    // The conflict path is uncommon but may carry a full segment's worth of
    // replay frames. Verify their ranges with bounded concurrency rather than
    // adding one object-store round trip at a time to takeover latency.
    futures::stream::iter(frames)
        .map(|frame| {
            let path = &path;
            async move {
                let end = frame
                    .byte_offset
                    .checked_add(frame.byte_len as u64)
                    .ok_or_else(|| {
                        SegmentStoreError::ObjectStore(format!(
                            "shipped frame range overflows for {segid:?}"
                        ))
                    })?;
                let existing = object_store
                    .get_range(path, frame.byte_offset..end)
                    .await
                    .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
                if existing != frame.bytes {
                    return Err(SegmentStoreError::ObjectStore(format!(
                        "existing segment {segid:?} disagrees with replayed frame {} for inode {} extent {}",
                        frame.frame_index, frame.inode, frame.extent
                    )));
                }
                Ok(())
            }
        })
        .buffer_unordered(SEAL_UPLOAD_CONCURRENCY)
        .try_collect::<Vec<()>>()
        .await?;
    Ok(())
}

/// Materialize a segment object from shipped frames unless another complete
/// object already owns the immutable key. The create is atomic: a delayed old
/// leader seal can win, but takeover never overwrites it with a possibly partial
/// reconstruction. Each frame's raw `[len][sealed]` bytes go back at its
/// original `byte_offset`, so the replayed `FrameLoc`s resolve. Returns whether
/// this call created the object. HA-takeover only.
pub async fn materialize_segment_if_absent(
    object_store: &Arc<dyn ObjectStore>,
    codec: &FrameCodec,
    segid: Segid,
    frames: &[ReconFrame],
) -> Result<bool> {
    if frames.is_empty() {
        return Ok(false);
    }
    let path = Path::from(segid.object_key());
    let end = frames.iter().try_fold(0u64, |end, frame| {
        let frame_end = frame
            .byte_offset
            .checked_add(frame.byte_len as u64)
            .ok_or_else(|| {
                SegmentStoreError::ObjectStore(format!(
                    "shipped frame range overflows for {segid:?}"
                ))
            })?;
        Ok::<_, SegmentStoreError>(end.max(frame_end))
    })?;
    let end = usize::try_from(end).map_err(|_| {
        SegmentStoreError::ObjectStore(format!("shipped segment is too large for {segid:?}"))
    })?;
    let mut buf = vec![0u8; end];
    for f in frames {
        let start = f.byte_offset as usize;
        let stop = start + f.byte_len as usize;
        if f.byte_len < LEN_PREFIX as u32
            || stop > buf.len()
            || f.bytes.len() != f.byte_len as usize
        {
            return Err(SegmentStoreError::ObjectStore(format!(
                "shipped frame layout mismatch for {segid:?}"
            )));
        }
        buf[start..stop].copy_from_slice(&f.bytes);
    }
    let mut sorted: Vec<&ReconFrame> = frames.iter().collect();
    sorted.sort_by_key(|f| f.frame_index);
    let dir: Vec<DirEntry> = sorted
        .iter()
        .map(|f| DirEntry {
            byte_offset: f.byte_offset,
            len: f.byte_len - LEN_PREFIX as u32,
            inode: f.inode,
            extent: f.extent,
        })
        .collect();
    let bytes = crate::segment::finalize_segment(codec, segid, buf, &dir, segid.counter)?;
    match object_store
        .put_opts(
            &path,
            Bytes::from(bytes).into(),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
            verify_existing_recon_segment(object_store, codec, segid, frames).await?;
            Ok(false)
        }
        Err(e) => Err(SegmentStoreError::ObjectStore(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompressionConfig;
    use crate::segment::SEGMENT_INFO;
    use futures::stream::BoxStream;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::{
        CopyOptions, GetResult, ListResult, ObjectMeta, PutMultipartOptions, PutOptions,
        PutPayload, PutResult, Result as OsResult, UploadPart,
    };
    use std::sync::atomic::AtomicBool;
    use tokio::sync::Notify;

    fn store() -> SegmentStore {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let codec = FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        SegmentStore::new(os, codec, 5, None)
    }

    #[tokio::test]
    async fn shared_pool_reservations_make_clone_writer_epochs_unique() {
        let pool: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority = SegmentStore::open_or_create_pool_authority(
            Arc::clone(&pool),
            &[1u8; 32],
            "source",
            true,
        )
        .await
        .unwrap();
        let epochs = futures::future::join_all((0..256).map(|index| {
            let pool = Arc::clone(&pool);
            let authority = authority.clone();
            async move {
                SegmentStore::reserve_epoch(pool, &authority, &format!("branch-{index}")).await
            }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
        let unique = epochs
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), epochs.len());

        let codec = || FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let first = SegmentStore::new(Arc::clone(&pool), codec(), epochs[0], None);
        let second = SegmentStore::new(Arc::clone(&pool), codec(), epochs[1], None);
        let first_locs = first
            .seal(&[(1, 0, Bytes::from_static(b"first"))])
            .await
            .unwrap();
        let second_locs = second
            .seal(&[(1, 0, Bytes::from_static(b"second"))])
            .await
            .unwrap();
        assert_ne!(first_locs[0].2.segid, second_locs[0].2.segid);
        assert_eq!(
            first.read_extent(first_locs[0].2, 1, 0).await.unwrap(),
            Bytes::from_static(b"first")
        );
        assert_eq!(
            second.read_extent(second_locs[0].2, 1, 0).await.unwrap(),
            Bytes::from_static(b"second")
        );
    }

    #[tokio::test]
    async fn offline_legacy_migration_is_exact_retryable_and_reserves_every_epoch() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = Path::from("volume/database");
        let database_instance_id = uuid::Uuid::new_v4();
        let wrapped_key_digest = [8u8; 32];
        let pool_path = Path::from("volume/segment-pool");
        let pool_store: Arc<dyn ObjectStore> =
            Arc::new(slatedb::object_store::prefix::PrefixStore::new(
                Arc::clone(&object_store),
                pool_path.clone(),
            ));
        pool_store
            .put(
                &Path::from("zerofs.key"),
                PutPayload::from(Bytes::from_static(b"exact wrapped key")),
            )
            .await
            .unwrap();
        let authority = SegmentStore::open_or_create_legacy_pool_authority(
            Arc::clone(&pool_store),
            &[9u8; 32],
            database.as_ref(),
            database_instance_id,
            wrapped_key_digest,
        )
        .await
        .unwrap();
        assert!(
            SegmentStore::legacy_migration_required(
                &pool_store,
                &authority,
                database.as_ref(),
                database_instance_id,
            )
            .await
            .unwrap(),
            "genesis itself durably requires completion before any intent exists"
        );
        let legacy_store: Arc<dyn ObjectStore> =
            Arc::new(slatedb::object_store::prefix::PrefixStore::new(
                Arc::clone(&object_store),
                database.clone(),
            ));
        let codec = || FrameCodec::new(&[9u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let epoch_seven = SegmentStore::new(Arc::clone(&legacy_store), codec(), 7, None);
        let epoch_eleven = SegmentStore::new(Arc::clone(&legacy_store), codec(), 11, None);
        let mut segments = Vec::new();
        segments.push(
            epoch_seven
                .seal(&[(1, 0, Bytes::from_static(b"legacy-one"))])
                .await
                .unwrap()[0]
                .2
                .segid,
        );
        segments.push(
            epoch_seven
                .seal(&[(2, 0, Bytes::from_static(b"legacy-two"))])
                .await
                .unwrap()[0]
                .2
                .segid,
        );
        segments.push(
            epoch_eleven
                .seal(&[(3, 0, Bytes::from_static(b"legacy-three"))])
                .await
                .unwrap()[0]
                .2
                .segid,
        );
        let expected_bytes = futures::future::try_join_all(segments.iter().map(|segid| {
            let object_store = Arc::clone(&object_store);
            let source = path_below(&database, &segid.object_key());
            async move {
                object_store
                    .get(&source)
                    .await?
                    .bytes()
                    .await
                    .map(|bytes| (*segid, bytes))
            }
        }))
        .await
        .unwrap();

        let first = SegmentStore::migrate_legacy_segments(
            Arc::clone(&object_store),
            &authority,
            &database,
            &pool_path,
            database_instance_id,
            wrapped_key_digest,
        )
        .await
        .unwrap();
        let retry = SegmentStore::migrate_legacy_segments(
            Arc::clone(&object_store),
            &authority,
            &database,
            &pool_path,
            database_instance_id,
            wrapped_key_digest,
        )
        .await
        .unwrap();
        assert_eq!(retry, first);
        assert_eq!(first.segment_count, segments.len() as u64);
        assert_eq!(
            first.total_bytes,
            expected_bytes
                .iter()
                .map(|(_, bytes)| bytes.len() as u64)
                .sum::<u64>()
        );
        assert_eq!(
            SegmentStore::legacy_migration_completion(
                &pool_store,
                &authority,
                database.as_ref(),
                database_instance_id,
                Some(wrapped_key_digest),
            )
            .await
            .unwrap(),
            Some(first)
        );
        assert!(
            SegmentStore::legacy_migration_required(
                &pool_store,
                &authority,
                database.as_ref(),
                uuid::Uuid::new_v4(),
            )
            .await
            .expect_err("path reuse must not bind a new database incarnation")
            .to_string()
            .contains("different legacy database incarnation")
        );
        SegmentStore::validate_epoch_reservations(Arc::clone(&pool_store), &authority)
            .await
            .unwrap();
        for (segid, bytes) in expected_bytes {
            assert_eq!(
                object_store
                    .get(&path_below(&pool_path, &segid.object_key()))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
                bytes
            );
        }
    }

    #[tokio::test]
    async fn offline_legacy_migration_rejects_cross_source_segment_ids() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first_database = Path::from("volume/first");
        let second_database = Path::from("volume/second");
        let pool_path = Path::from("volume/segment-pool");
        let pool_store: Arc<dyn ObjectStore> =
            Arc::new(slatedb::object_store::prefix::PrefixStore::new(
                Arc::clone(&object_store),
                pool_path.clone(),
            ));
        pool_store
            .put(
                &Path::from("zerofs.key"),
                PutPayload::from(Bytes::from_static(b"exact wrapped key")),
            )
            .await
            .unwrap();
        let authority = SegmentStore::open_or_create_pool_authority(
            Arc::clone(&pool_store),
            &[4u8; 32],
            first_database.as_ref(),
            true,
        )
        .await
        .unwrap();
        let codec = || FrameCodec::new(&[4u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let first_instance_id = uuid::Uuid::new_v4();
        let second_instance_id = uuid::Uuid::new_v4();
        let wrapped_key_digest = [7u8; 32];
        let mut segid = None;
        for (database, bytes) in [
            (&first_database, Bytes::from_static(b"first source")),
            (&second_database, Bytes::from_static(b"second source")),
        ] {
            let legacy_store: Arc<dyn ObjectStore> =
                Arc::new(slatedb::object_store::prefix::PrefixStore::new(
                    Arc::clone(&object_store),
                    database.clone(),
                ));
            let written = SegmentStore::new(legacy_store, codec(), 5, None)
                .seal(&[(1, 0, bytes)])
                .await
                .unwrap();
            assert_eq!(segid.get_or_insert(written[0].2.segid), &written[0].2.segid);
        }
        let migrate_first = || {
            SegmentStore::migrate_legacy_segments(
                Arc::clone(&object_store),
                &authority,
                &first_database,
                &pool_path,
                first_instance_id,
                wrapped_key_digest,
            )
        };
        let migrate_second = || {
            SegmentStore::migrate_legacy_segments(
                Arc::clone(&object_store),
                &authority,
                &second_database,
                &pool_path,
                second_instance_id,
                wrapped_key_digest,
            )
        };
        let (first, second) = tokio::join!(migrate_first(), migrate_second());
        assert_ne!(
            first.is_ok(),
            second.is_ok(),
            "exactly one source may own the Segid"
        );
        let (first_retry, second_retry) = tokio::join!(migrate_first(), migrate_second());
        assert_eq!(first.is_ok(), first_retry.is_ok());
        assert_eq!(second.is_ok(), second_retry.is_ok());
        let loser = match (first_retry, second_retry) {
            (Err(error), Ok(_)) | (Ok(_), Err(error)) => error,
            _ => panic!("exactly one retry must retain global Segid ownership"),
        };
        let loser = loser.to_string();
        assert!(
            loser.contains("conflicts with this migration")
                || loser.contains("duplicate physical segment"),
            "unexpected collision error: {loser}"
        );
    }

    #[tokio::test]
    async fn offline_legacy_retry_rejects_a_removed_durable_claim() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = Path::from("volume/interrupted");
        let database_instance_id = uuid::Uuid::new_v4();
        let wrapped_key_digest = [6u8; 32];
        let pool_path = Path::from("volume/segment-pool");
        let pool_store: Arc<dyn ObjectStore> =
            Arc::new(slatedb::object_store::prefix::PrefixStore::new(
                Arc::clone(&object_store),
                pool_path.clone(),
            ));
        pool_store
            .put(
                &Path::from("zerofs.key"),
                PutPayload::from(Bytes::from_static(b"exact wrapped key")),
            )
            .await
            .unwrap();
        let authority = SegmentStore::open_or_create_legacy_pool_authority(
            Arc::clone(&pool_store),
            &[6u8; 32],
            database.as_ref(),
            database_instance_id,
            wrapped_key_digest,
        )
        .await
        .unwrap();
        let legacy_store: Arc<dyn ObjectStore> =
            Arc::new(slatedb::object_store::prefix::PrefixStore::new(
                Arc::clone(&object_store),
                database.clone(),
            ));
        let codec = FrameCodec::new(&[6u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let segid = SegmentStore::new(legacy_store, codec, 12, None)
            .seal(&[(1, 0, Bytes::from_static(b"claimed-before-crash"))])
            .await
            .unwrap()[0]
            .2
            .segid;
        let source = path_below(&database, &segid.object_key());
        let target = path_below(&pool_path, &segid.object_key());
        let (size, content_digest) = digest_object(&object_store, &source).await.unwrap();
        let migration_id = legacy_migration_id(&authority, database.as_ref());
        let mut initial = LegacyInventory::default();
        initial.include(segid, size, content_digest).unwrap();
        let mut intent = LegacyMigrationIntent {
            schema_version: LEGACY_MIGRATION_VERSION,
            pool_id: authority.pool_id,
            migration_id,
            database_identity: database.to_string(),
            database_instance_id,
            wrapped_key_digest,
            initial_segment_count: initial.segment_count,
            initial_total_bytes: initial.total_bytes,
            initial_inventory_fingerprint: initial.fingerprint,
            auth_tag: [0; 32],
        };
        intent.auth_tag = legacy_intent_tag(&authority, &intent);
        create_json_exact(&pool_store, &legacy_intent_path(migration_id), &intent)
            .await
            .unwrap();
        let source_bytes = object_store
            .get(&source)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        object_store.delete(&source).await.unwrap();
        let error = SegmentStore::migrate_legacy_segments(
            Arc::clone(&object_store),
            &authority,
            &database,
            &pool_path,
            database_instance_id,
            wrapped_key_digest,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("durable migration intent or claims")
        );
        object_store
            .put(&source, PutPayload::from(source_bytes))
            .await
            .unwrap();
        SegmentStore::reserve_imported_epoch(Arc::clone(&pool_store), &authority, segid.epoch)
            .await
            .unwrap();
        let mut claim = LegacySegmentClaim {
            schema_version: LEGACY_MIGRATION_VERSION,
            pool_id: authority.pool_id,
            migration_id,
            database_identity: database.to_string(),
            database_instance_id,
            wrapped_key_digest,
            segid,
            size,
            content_digest,
            auth_tag: [0; 32],
        };
        claim.auth_tag = legacy_claim_tag(&authority, &claim);
        create_json_exact(&pool_store, &legacy_claim_path(segid), &claim)
            .await
            .unwrap();
        object_store
            .copy_opts(
                &source,
                &target,
                CopyOptions {
                    mode: CopyMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        object_store.delete(&source).await.unwrap();

        let error = SegmentStore::migrate_legacy_segments(
            object_store,
            &authority,
            &database,
            &pool_path,
            database_instance_id,
            wrapped_key_digest,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("durable migration intent or claims")
        );
        assert!(
            SegmentStore::legacy_migration_completion(
                &pool_store,
                &authority,
                database.as_ref(),
                database_instance_id,
                Some(wrapped_key_digest),
            )
            .await
            .unwrap()
            .is_none()
        );
    }

    #[tokio::test]
    async fn branch_epoch_reservation_authenticates_exact_incarnation() {
        let pool: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority =
            SegmentStore::open_or_create_pool_authority(pool.clone(), &[1u8; 32], "volume", true)
                .await
                .unwrap();
        let branch_id = uuid::Uuid::new_v4();
        let epoch = SegmentStore::reserve_branch_epoch(
            pool.clone(),
            &authority,
            "branches/exact-incarnation",
            branch_id,
        )
        .await
        .unwrap();
        let segid = Segid::new(epoch, 0);
        let authenticated =
            SegmentStore::authenticate_branch_epoch(pool.clone(), &authority, epoch)
                .await
                .unwrap();
        assert_eq!(authenticated.epoch, epoch);
        assert_eq!(authenticated.branch_id, branch_id);
        assert_eq!(
            authenticated.database_identity,
            "branches/exact-incarnation"
        );
        pool.put(
            &Path::from(segid.object_key()),
            Bytes::from_static(b"branch-owned epoch segment").into(),
        )
        .await
        .unwrap();
        SegmentStore::validate_epoch_reservations(pool.clone(), &authority)
            .await
            .unwrap();

        let marker_path = Path::from(format!("{SEGMENT_EPOCH_PREFIX}/{epoch:016x}.json"));
        let mut marker: SegmentEpochReservation =
            serde_json::from_slice(&pool.get(&marker_path).await.unwrap().bytes().await.unwrap())
                .unwrap();
        assert_eq!(marker.branch_id, Some(branch_id));
        marker.branch_id = Some(uuid::Uuid::new_v4());
        pool.put(&marker_path, serde_json::to_vec(&marker).unwrap().into())
            .await
            .unwrap();
        assert!(
            SegmentStore::validate_epoch_reservations(pool.clone(), &authority)
                .await
                .unwrap_err()
                .to_string()
                .contains("mismatched reservation marker")
        );
        assert!(
            SegmentStore::authenticate_branch_epoch(pool, &authority, epoch)
                .await
                .unwrap_err()
                .to_string()
                .contains("mismatched reservation marker")
        );
    }

    #[tokio::test]
    async fn legacy_authenticated_epoch_reservation_remains_global_only() {
        let pool: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority = SegmentStore::open_or_create_pool_authority(
            pool.clone(),
            &[1u8; 32],
            "legacy-volume",
            true,
        )
        .await
        .unwrap();
        let epoch = 17;
        let reservation_id = uuid::Uuid::new_v4();
        let marker = SegmentEpochReservation {
            schema_version: LEGACY_SEGMENT_EPOCH_RESERVATION_VERSION,
            pool_id: authority.pool_id,
            epoch,
            reservation_id,
            database_identity: "legacy-database".to_string(),
            branch_id: None,
            auth_tag: reservation_tag_v1(&authority, epoch, reservation_id, "legacy-database"),
        };
        pool.put(
            &Path::from(format!("{SEGMENT_EPOCH_PREFIX}/{epoch:016x}.json")),
            serde_json::to_vec(&marker).unwrap().into(),
        )
        .await
        .unwrap();
        pool.put(
            &Path::from(Segid::new(epoch, 0).object_key()),
            Bytes::from_static(b"legacy epoch segment").into(),
        )
        .await
        .unwrap();

        SegmentStore::validate_epoch_reservations(pool, &authority)
            .await
            .unwrap();
        assert!(marker.branch_id.is_none());
    }

    #[tokio::test]
    async fn shared_pool_genesis_is_key_authenticated_and_empty_only() {
        let interrupted: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        SegmentStore::mark_legacy_pool_bootstrap(
            &interrupted,
            "legacy/interrupted",
            uuid::Uuid::new_v4(),
        )
        .await
        .unwrap();
        let interrupted_error = match SegmentStore::open_or_create_pool_authority(
            interrupted,
            &[3u8; 32],
            "legacy/interrupted",
            true,
        )
        .await
        {
            Ok(_) => panic!("bootstrap must block native genesis"),
            Err(error) => error,
        };
        assert!(
            interrupted_error.to_string().contains("cannot establish"),
            "ordinary startup cannot reinterpret a pre-genesis migration crash"
        );

        let fresh_read_only: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert!(
            SegmentStore::open_or_create_pool_authority(
                fresh_read_only,
                &[3u8; 32],
                "reader",
                false,
            )
            .await
            .err()
            .expect("read-only startup must not establish genesis")
            .to_string()
            .contains("read-only startup")
        );

        let pool: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        pool.put(
            &Path::from("zerofs.key"),
            Bytes::from_static(b"wrapped-key-placeholder").into(),
        )
        .await
        .unwrap();
        let authority = SegmentStore::open_or_create_pool_authority(
            Arc::clone(&pool),
            &[3u8; 32],
            "source",
            true,
        )
        .await
        .unwrap();
        let reopened = SegmentStore::open_or_create_pool_authority(
            Arc::clone(&pool),
            &[3u8; 32],
            "another-branch",
            false,
        )
        .await
        .unwrap();
        assert_eq!(authority.pool_id, reopened.pool_id);
        assert!(
            SegmentStore::open_or_create_pool_authority(pool, &[4u8; 32], "wrong-key", false)
                .await
                .err()
                .expect("a different volume key must not authenticate the genesis")
                .to_string()
                .contains("unauthenticated")
        );

        let prepopulated: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        prepopulated
            .put(
                &Path::from(Segid::new(7, 0).object_key()),
                Bytes::from_static(b"legacy").into(),
            )
            .await
            .unwrap();
        assert!(
            SegmentStore::open_or_create_pool_authority(
                prepopulated,
                &[3u8; 32],
                "legacy-source",
                true,
            )
            .await
            .err()
            .expect("a nonempty pool must not gain new-volume genesis")
            .to_string()
            .contains("cannot establish")
        );
    }

    #[tokio::test]
    async fn shared_pool_rejects_segments_without_exact_epoch_reservations() {
        let pool: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority = SegmentStore::open_or_create_pool_authority(
            Arc::clone(&pool),
            &[1u8; 32],
            "source",
            true,
        )
        .await
        .unwrap();
        let segid = Segid::new(7, 0);
        pool.put(
            &Path::from(segid.object_key()),
            Bytes::from_static(b"manually-copied-legacy-segment").into(),
        )
        .await
        .unwrap();
        assert!(
            SegmentStore::validate_epoch_reservations(Arc::clone(&pool), &authority)
                .await
                .unwrap_err()
                .to_string()
                .contains("no readable permanent reservation")
        );

        let marker = SegmentEpochReservation {
            schema_version: SEGMENT_EPOCH_RESERVATION_VERSION,
            pool_id: authority.pool_id,
            epoch: 7,
            reservation_id: uuid::Uuid::new_v4(),
            database_identity: "forged-migration".to_string(),
            branch_id: None,
            auth_tag: [0u8; 32],
        };
        pool.put(
            &Path::from(format!("{SEGMENT_EPOCH_PREFIX}/0000000000000007.json")),
            serde_json::to_vec(&marker).unwrap().into(),
        )
        .await
        .unwrap();
        assert!(
            SegmentStore::validate_epoch_reservations(pool, &authority)
                .await
                .unwrap_err()
                .to_string()
                .contains("mismatched reservation marker")
        );
    }

    /// `len` bytes of keyed xorshift noise: incompressible, so a seal built
    /// from it stays past the single-PUT threshold.
    fn noise(seed: u64, len: usize) -> Bytes {
        let mut x = seed | 1;
        let mut v = Vec::with_capacity(len + 8);
        while v.len() < len {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            v.extend_from_slice(&x.to_le_bytes());
        }
        v.truncate(len);
        Bytes::from(v)
    }

    fn recon_frames(
        frames: &[(InodeId, u64, Bytes)],
        locs: &[(InodeId, u64, FrameLoc)],
        segment: &Bytes,
    ) -> Vec<ReconFrame> {
        frames
            .iter()
            .zip(locs)
            .map(|((inode, extent, _), (_, _, loc))| {
                let start = loc.byte_offset as usize;
                let end = start + loc.byte_len as usize;
                ReconFrame {
                    frame_index: loc.frame_index,
                    byte_offset: loc.byte_offset,
                    byte_len: loc.byte_len,
                    inode: *inode,
                    extent: *extent,
                    bytes: segment.slice(start..end),
                }
            })
            .collect()
    }

    /// Wraps `InMemory` to inject multipart or create failures and record the
    /// resulting cleanup behavior.
    #[derive(Debug)]
    struct MultipartFaultStore {
        inner: Arc<dyn ObjectStore>,
        fail_part: Option<usize>,
        fail_complete: bool,
        fail_put: bool,
        fail_after_create: AtomicBool,
        aborted: Arc<AtomicBool>,
    }

    impl MultipartFaultStore {
        fn new(fail_part: Option<usize>, fail_complete: bool) -> (Arc<Self>, Arc<AtomicBool>) {
            let aborted = Arc::new(AtomicBool::new(false));
            (
                Arc::new(Self {
                    inner: Arc::new(InMemory::new()),
                    fail_part,
                    fail_complete,
                    fail_put: false,
                    fail_after_create: AtomicBool::new(false),
                    aborted: aborted.clone(),
                }),
                aborted,
            )
        }

        fn with_create_failure(fail_after_create: bool) -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(InMemory::new()),
                fail_part: None,
                fail_complete: false,
                fail_put: !fail_after_create,
                fail_after_create: AtomicBool::new(fail_after_create),
                aborted: Arc::new(AtomicBool::new(false)),
            })
        }

        fn injected() -> slatedb::object_store::Error {
            slatedb::object_store::Error::Generic {
                store: "MultipartFaultStore",
                source: "injected object-store fault".into(),
            }
        }
    }

    impl std::fmt::Display for MultipartFaultStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MultipartFaultStore({})", self.inner)
        }
    }

    #[derive(Debug)]
    struct FaultUpload {
        inner: Box<dyn MultipartUpload>,
        fail_part: Option<usize>,
        fail_complete: bool,
        next_part: usize,
        aborted: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl MultipartUpload for FaultUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            let idx = self.next_part;
            self.next_part += 1;
            if self.fail_part == Some(idx) {
                return Box::pin(futures::future::ready(Err(MultipartFaultStore::injected())));
            }
            self.inner.put_part(data)
        }

        async fn complete(&mut self) -> OsResult<PutResult> {
            if self.fail_complete {
                return Err(MultipartFaultStore::injected());
            }
            self.inner.complete().await
        }

        async fn abort(&mut self) -> OsResult<()> {
            self.aborted.store(true, Ordering::SeqCst);
            self.inner.abort().await
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for MultipartFaultStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            if self.fail_put {
                return Err(Self::injected());
            }
            let is_create = matches!(&opts.mode, PutMode::Create);
            let result = self.inner.put_opts(location, payload, opts).await?;
            if is_create && self.fail_after_create.swap(false, Ordering::SeqCst) {
                return Err(Self::injected());
            }
            Ok(result)
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            let inner = self.inner.put_multipart_opts(location, opts).await?;
            Ok(Box::new(FaultUpload {
                inner,
                fail_part: self.fail_part,
                fail_complete: self.fail_complete,
                next_part: 0,
                aborted: self.aborted.clone(),
            }))
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, OsResult<Path>>,
        ) -> BoxStream<'static, OsResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Pauses one conditional create before it reaches the backing store, so a
    /// competing old-leader seal can deterministically win the object key.
    #[derive(Debug)]
    struct CreateGateStore {
        inner: Arc<dyn ObjectStore>,
        entered: Notify,
        release: Notify,
        gate_once: AtomicBool,
    }

    impl CreateGateStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                entered: Notify::new(),
                release: Notify::new(),
                gate_once: AtomicBool::new(true),
            })
        }
    }

    impl std::fmt::Display for CreateGateStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CreateGateStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CreateGateStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            if matches!(&opts.mode, PutMode::Create) && self.gate_once.swap(false, Ordering::SeqCst)
            {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, OsResult<Path>>,
        ) -> BoxStream<'static, OsResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn seal_then_read_roundtrips() {
        let store = store();
        let frames = vec![
            (10u64, 0u64, Bytes::from(vec![1u8; 1000])),
            (10, 1, Bytes::from(vec![2u8; 2000])),
            (20, 0, Bytes::from(vec![3u8; 500])),
        ];
        let locs = store.seal(&frames).await.unwrap();
        assert_eq!(locs.len(), 3);
        for ((id, extent, data), (lid, lextent, loc)) in frames.iter().zip(&locs) {
            assert_eq!(id, lid);
            assert_eq!(extent, lextent);
            let got = store.read_extent(*loc, *id, *extent).await.unwrap();
            assert_eq!(&got, data);
        }
    }

    #[tokio::test]
    async fn read_under_wrong_slot_fails() {
        let store = store();
        let locs = store
            .seal(&[(10u64, 0u64, Bytes::from(vec![7u8; 100]))])
            .await
            .unwrap();
        let (_, _, loc) = locs[0];
        assert!(store.read_extent(loc, 999, 999).await.is_err());
    }

    #[tokio::test]
    async fn distinct_seals_get_distinct_segids() {
        let store = store();
        let a = store
            .seal(&[(1, 0, Bytes::from_static(b"a"))])
            .await
            .unwrap();
        let b = store
            .seal(&[(1, 0, Bytes::from_static(b"b"))])
            .await
            .unwrap();
        assert_ne!(a[0].2.segid, b[0].2.segid, "counter must advance per seal");
        assert_eq!(
            store.read_extent(a[0].2, 1, 0).await.unwrap().as_ref(),
            b"a"
        );
        assert_eq!(
            store.read_extent(b[0].2, 1, 0).await.unwrap().as_ref(),
            b"b"
        );
    }

    // A seal past the single-PUT threshold streams as concurrent multipart and
    // must read back byte-identically.
    #[tokio::test]
    async fn multipart_seal_roundtrips() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let codec = FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let store = SegmentStore::new(os.clone(), codec, 5, None);
        let frames: Vec<(u64, u64, Bytes)> =
            (0..4u64).map(|i| (30, i, noise(i + 1, 4 << 20))).collect();
        let locs = store.seal(&frames).await.unwrap();
        let segid = locs[0].2.segid;
        let size = os.head(&Path::from(segid.object_key())).await.unwrap().size;
        assert!(
            size as usize > SEAL_PART_SIZE,
            "seal must take the multipart path"
        );
        for ((id, extent, data), (_, _, loc)) in frames.iter().zip(&locs) {
            let got = store.read_extent(*loc, *id, *extent).await.unwrap();
            assert_eq!(&got, data);
        }
    }

    #[tokio::test]
    async fn segment_put_is_immutable_and_exact_retry_is_idempotent() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let codec = FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let store = SegmentStore::new(os.clone(), codec, 5, None);
        let segid = store.next_segid();
        let first = Bytes::from_static(b"first immutable segment bytes");

        store.put_segment(segid, first.clone()).await.unwrap();
        store.put_segment(segid, first.clone()).await.unwrap();
        let error = store
            .put_segment(segid, Bytes::from_static(b"other immutable segment bytes"))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("immutable segment key"));
        assert_eq!(
            os.get(&Path::from(segid.object_key()))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            first
        );
    }

    // This is the writer-side half of GC's identity-check/delete contract:
    // even concurrent legitimate writers cannot replace an existing segment
    // key between those operations. Exactly one different payload can win.
    #[tokio::test]
    async fn concurrent_segment_puts_cannot_replace_the_winner() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let codec = || FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let first_store = SegmentStore::new(os.clone(), codec(), 5, None);
        let second_store = SegmentStore::new(os.clone(), codec(), 5, None);
        let segid = Segid::new(5, 99);
        let first = Bytes::from_static(b"first concurrent segment");
        let second = Bytes::from_static(b"other concurrent segment");

        let (first_result, second_result) = tokio::join!(
            first_store.put_segment(segid, first.clone()),
            second_store.put_segment(segid, second.clone())
        );
        assert_ne!(first_result.is_ok(), second_result.is_ok());
        let stored = os
            .get(&Path::from(segid.object_key()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(stored == first || stored == second);
    }

    #[tokio::test]
    async fn multipart_segment_copy_is_create_only() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let codec = FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let store = SegmentStore::new(os.clone(), codec, 5, None);
        let segid = store.next_segid();
        let first = noise(1, SEAL_PART_SIZE);
        let different = noise(2, SEAL_PART_SIZE);

        store.put_segment(segid, first.clone()).await.unwrap();
        store.put_segment(segid, first.clone()).await.unwrap();
        let error = store.put_segment(segid, different).await.unwrap_err();

        assert!(error.to_string().contains("immutable segment key"));
        assert_eq!(
            os.get(&Path::from(segid.object_key()))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            first
        );
        assert!(
            os.list(Some(&Path::from(SEGMENT_UPLOAD_PREFIX)))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty(),
            "completed multipart staging objects are cleaned up"
        );
    }

    // A part failing mid-multipart must abort the upload on the way out: an
    // unaborted upload's parts are invisible to LIST — and so to the orphan
    // sweep — yet billed until aborted, and every retried seal would strand a
    // fresh batch.
    #[tokio::test]
    async fn failed_multipart_part_aborts_the_upload() {
        let (os, aborted) = MultipartFaultStore::new(Some(1), false);
        let codec = FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let store = SegmentStore::new(os, codec, 5, None);
        let res = store
            .put_segment(store.next_segid(), noise(1, 2 * SEAL_PART_SIZE + 1024))
            .await;
        assert!(res.is_err(), "the seal error must surface");
        assert!(
            aborted.load(Ordering::SeqCst),
            "failed seal must abort its multipart upload"
        );
    }

    // COMPLETE failing after every part landed must abort too: those parts are
    // already durable on the backend, just as invisible and billed.
    #[tokio::test]
    async fn failed_multipart_complete_aborts_the_upload() {
        let (os, aborted) = MultipartFaultStore::new(None, true);
        let codec = FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let store = SegmentStore::new(os, codec, 5, None);
        let res = store
            .put_segment(store.next_segid(), noise(1, 2 * SEAL_PART_SIZE + 1024))
            .await;
        assert!(res.is_err(), "the seal error must surface");
        assert!(
            aborted.load(Ordering::SeqCst),
            "failed COMPLETE must abort its multipart upload"
        );
    }

    // The listing fans out one LIST per shard prefix; every sealed segment must
    // come back exactly once, whichever of the 256 shards its counter lands in.
    #[tokio::test]
    async fn list_segments_covers_all_shards_exactly_once() {
        let store = store();
        let mut expect = Vec::new();
        for i in 0..20u64 {
            let locs = store
                .seal(&[(i, 0, Bytes::from(vec![i as u8; 64]))])
                .await
                .unwrap();
            expect.push(locs[0].2.segid);
        }
        let mut listed = store.list_segments().await.unwrap();
        listed.sort_by_key(|s| (s.epoch, s.counter));
        expect.sort_by_key(|s| (s.epoch, s.counter));
        assert_eq!(listed, expect, "20 seals span 20 shard prefixes");
    }

    // Two databases sharing one bucket must not see — or clobber — each other's
    // segments. Both writers start at the same epoch, so their segids collide
    // (segments/<shard>/<e>/<c> is identical); only the per-db path prefix keeps the
    // objects apart. Without it, db_b's seal overwrites db_a's at the shared key.
    #[tokio::test]
    async fn segments_are_isolated_by_db_path_prefix() {
        use slatedb::object_store::prefix::PrefixStore;

        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let codec = || FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let store_a: Arc<dyn ObjectStore> =
            Arc::new(PrefixStore::new(bucket.clone(), Path::from("db_a")));
        let store_b: Arc<dyn ObjectStore> =
            Arc::new(PrefixStore::new(bucket.clone(), Path::from("db_b")));
        let seg_a = SegmentStore::new(store_a, codec(), 5, None);
        let seg_b = SegmentStore::new(store_b, codec(), 5, None);

        let a = seg_a
            .seal(&[(1, 0, Bytes::from_static(b"aaaa"))])
            .await
            .unwrap();
        let b = seg_b
            .seal(&[(1, 0, Bytes::from_static(b"bbbb"))])
            .await
            .unwrap();
        assert_eq!(
            a[0].2.segid, b[0].2.segid,
            "fresh dbs reuse the same segids"
        );

        // Reads go straight to the object store (no in-RAM segment cache).
        assert_eq!(
            seg_a.read_extent(a[0].2, 1, 0).await.unwrap().as_ref(),
            b"aaaa"
        );
        assert_eq!(
            seg_b.read_extent(b[0].2, 1, 0).await.unwrap().as_ref(),
            b"bbbb"
        );

        // Each db lists only its own segment, not the other's.
        assert_eq!(seg_a.list_segments().await.unwrap().len(), 1);
        assert_eq!(seg_b.list_segments().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn materialization_propagates_create_errors() {
        let object_store: Arc<dyn ObjectStore> = MultipartFaultStore::with_create_failure(false);
        let codec = FrameCodec::new(&[3u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let segid = Segid::new(9, 1);
        let frames = [ReconFrame {
            frame_index: 0,
            byte_offset: 0,
            byte_len: 5,
            inode: 1,
            extent: 0,
            bytes: Bytes::from_static(b"\x01\0\0\0x"),
        }];

        let err = materialize_segment_if_absent(&object_store, &codec, segid, &frames)
            .await
            .unwrap_err();
        assert!(matches!(err, SegmentStoreError::ObjectStore(_)));
        assert!(matches!(
            object_store.head(&Path::from(segid.object_key())).await,
            Err(slatedb::object_store::Error::NotFound { .. })
        ));
    }

    // HA takeover: the bytes the leader ships (a segment's raw frames) reconstruct
    // a segment on a fresh store that reads back identically — and atomic create
    // makes a repeat call a verified no-op.
    #[tokio::test]
    async fn shipped_frames_reconstruct_a_readable_segment() {
        let codec = || FrameCodec::new(&[3u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);

        // Leader: seal frames into store A; that object's bytes are what's shipped.
        let store_a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let seg_a = SegmentStore::new(store_a.clone(), codec(), 9, None);
        let frames = vec![
            (5u64, 0u64, Bytes::from(vec![1u8; 1000])),
            (5, 1, Bytes::from(vec![2u8; 2000])),
            (7, 0, Bytes::from(vec![3u8; 1500])),
        ];
        let locs = seg_a.seal(&frames).await.unwrap();
        let segid = locs[0].2.segid;
        let seg_bytes = store_a
            .get(&Path::from(segid.object_key()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        // Standby: rebuild ReconFrames from each FrameLoc + the shipped raw bytes.
        let recon = recon_frames(&frames, &locs, &seg_bytes);

        let store_b: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert!(
            materialize_segment_if_absent(&store_b, &codec(), segid, &recon)
                .await
                .unwrap(),
            "absent on the fresh store -> materialized"
        );
        assert!(
            !materialize_segment_if_absent(&store_b, &codec(), segid, &recon)
                .await
                .unwrap(),
            "verified existing object -> no-op"
        );

        let seg_b = SegmentStore::new(store_b, codec(), 9, None);
        for ((inode, extent, data), (_, _, loc)) in frames.iter().zip(&locs) {
            let got = seg_b.read_extent(*loc, *inode, *extent).await.unwrap();
            assert_eq!(&got, data, "reconstructed frame reads back identically");
        }
    }

    // The old leader may already be sealing the full segment when takeover
    // reconstructs a subset from its replication tail. If the full PUT lands
    // first, takeover's create must preserve it rather than overwrite it with
    // the partial object it prepared before discovering the winner.
    #[tokio::test]
    async fn concurrent_full_seal_wins_over_partial_takeover_reconstruction() {
        let codec = || FrameCodec::new(&[3u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let leader_os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let leader = SegmentStore::new(leader_os.clone(), codec(), 9, None);
        let frames = vec![
            (5u64, 0u64, Bytes::from(vec![1u8; 1000])),
            (5, 1, Bytes::from(vec![2u8; 2000])),
            (7, 0, Bytes::from(vec![3u8; 1500])),
        ];
        let locs = leader.seal(&frames).await.unwrap();
        let segid = locs[0].2.segid;
        let path = Path::from(segid.object_key());
        let full = leader_os.get(&path).await.unwrap().bytes().await.unwrap();
        let partial_recon = recon_frames(&frames[..2], &locs[..2], &full);

        let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gate = CreateGateStore::new(shared.clone());
        let takeover_store: Arc<dyn ObjectStore> = gate.clone();
        let takeover_codec = codec();
        let takeover =
            materialize_segment_if_absent(&takeover_store, &takeover_codec, segid, &partial_recon);
        let old_leader = async {
            gate.entered.notified().await;
            shared.put(&path, full.clone().into()).await.unwrap();
            gate.release.notify_one();
        };
        let (materialized, ()) = tokio::join!(takeover, old_leader);
        assert!(
            !materialized.unwrap(),
            "the old leader won the immutable segment key"
        );
        assert_eq!(
            shared.get(&path).await.unwrap().bytes().await.unwrap(),
            full,
            "takeover must not replace the full seal with its partial reconstruction"
        );

        let reader = SegmentStore::new(shared, codec(), 9, None);
        for ((inode, extent, expected), (_, _, loc)) in frames.iter().zip(&locs) {
            assert_eq!(
                reader.read_extent(*loc, *inode, *extent).await.unwrap(),
                expected
            );
        }
    }

    // A create can land while its success response is lost. Retrying the same
    // conditional request then returns AlreadyExists; byte verification turns
    // that ambiguous response into success without a second overwrite.
    #[tokio::test]
    async fn lost_create_response_is_verified_as_success() {
        let codec = || FrameCodec::new(&[3u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let source_os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source = SegmentStore::new(source_os.clone(), codec(), 9, None);
        let frames = vec![(5u64, 0u64, Bytes::from(vec![1u8; 1000]))];
        let locs = source.seal(&frames).await.unwrap();
        let segid = locs[0].2.segid;
        let path = Path::from(segid.object_key());
        let segment = source_os.get(&path).await.unwrap().bytes().await.unwrap();
        let recon = recon_frames(&frames, &locs, &segment);

        let fault = MultipartFaultStore::with_create_failure(true);
        let retrying: Arc<dyn ObjectStore> = Arc::new(
            crate::retrying_object_store::RetryingObjectStore::new(fault.clone()),
        );
        assert!(
            !materialize_segment_if_absent(&retrying, &codec(), segid, &recon)
                .await
                .unwrap(),
            "the retry observes the object created by the attempt whose response was lost"
        );
        let reader = SegmentStore::new(fault.inner.clone(), codec(), 9, None);
        assert_eq!(
            reader
                .read_extent(locs[0].2, frames[0].0, frames[0].1)
                .await
                .unwrap(),
            frames[0].2
        );
    }

    #[tokio::test]
    async fn existing_segment_mismatch_aborts_takeover() {
        let codec = || FrameCodec::new(&[3u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let wanted_os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wanted = SegmentStore::new(wanted_os.clone(), codec(), 9, None);
        let wanted_frames = vec![(5u64, 0u64, Bytes::from(vec![1u8; 1000]))];
        let wanted_locs = wanted.seal(&wanted_frames).await.unwrap();
        let segid = wanted_locs[0].2.segid;
        let path = Path::from(segid.object_key());
        let wanted_bytes = wanted_os.get(&path).await.unwrap().bytes().await.unwrap();
        let recon = recon_frames(&wanted_frames, &wanted_locs, &wanted_bytes);

        // A fresh writer with the same epoch/counter produces the same key but
        // different sealed bytes. Create must not overwrite it, and verification
        // must not let replay publish a pointer into the conflicting object.
        let existing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let conflicting = SegmentStore::new(existing.clone(), codec(), 9, None);
        let conflicting_frames = vec![(5u64, 0u64, Bytes::from(vec![9u8; 1000]))];
        let conflicting_locs = conflicting.seal(&conflicting_frames).await.unwrap();
        assert_eq!(conflicting_locs[0].2.segid, segid);
        let before = existing.get(&path).await.unwrap().bytes().await.unwrap();

        let err = materialize_segment_if_absent(&existing, &codec(), segid, &recon)
            .await
            .unwrap_err();
        assert!(matches!(err, SegmentStoreError::ObjectStore(_)));
        assert_eq!(
            existing.get(&path).await.unwrap().bytes().await.unwrap(),
            before,
            "failed verification must leave the existing object untouched"
        );
    }
}
