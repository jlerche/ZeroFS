use crate::checkpoint_manager::CheckpointManager;
use crate::config::{NbdConfig, NfsConfig, NinePConfig, RpcConfig, Settings};
use crate::db::SlateDbHandle;
use crate::fs::permissions::Credentials;
use crate::fs::types::SetAttributes;
use crate::fs::{CacheConfig, GarbageCollector, ZeroFS};
use crate::length_checked_object_store::LengthCheckedObjectStore;
use crate::nbd::NBDServer;
use crate::object_store_prefetch::PrefetchingObjectStore;
use crate::parse_object_store::parse_url_opts;
use crate::storage_class_object_store::with_storage_class;
use crate::task::spawn_named;
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig,
    S3FifoConfig, Spawner,
};
use slatedb::admin::AdminBuilder;
use slatedb::config::GarbageCollectorDirectoryOptions;
use slatedb::config::GarbageCollectorOptions;
use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
use slatedb::object_store::path::Path;
use slatedb::{BlockTransformer, CompactorBuilder, DbBuilder, DbReader, DbReaderMode};
use slatedb_common::metrics::DefaultMetricsRecorder;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

/// Parse a WAL config into an object store rooted at the full URL path.
pub(crate) fn parse_wal_object_store(
    wal_config: &crate::config::WalConfig,
) -> Result<Arc<dyn object_store::ObjectStore>> {
    let env_vars = wal_config.cloud_provider_env_vars();
    let (store, path) = parse_url_opts(&wal_config.url.parse()?, env_vars)?;
    let path_str: &str = path.as_ref();
    let store: Arc<dyn object_store::ObjectStore> = if path_str.is_empty() {
        Arc::from(store)
    } else {
        Arc::new(object_store::prefix::PrefixStore::new(store, path))
    };
    Ok(with_storage_class(
        store,
        wal_config.storage_class.as_deref(),
    ))
}

#[derive(Debug, Clone, Copy)]
pub enum DatabaseMode {
    ReadWrite,
    ReadOnly,
    Checkpoint(uuid::Uuid),
}

impl DatabaseMode {
    pub fn is_read_only(&self) -> bool {
        !matches!(self, DatabaseMode::ReadWrite)
    }
}

async fn resolve_checkpoint_name(settings: &Settings, name: &str) -> Result<uuid::Uuid> {
    let env_vars = settings.cloud_provider_env_vars();
    let (object_store, path_from_url) = parse_url_opts(&settings.storage.url.parse()?, env_vars)?;
    let object_store = with_storage_class(
        Arc::from(object_store),
        settings.storage.storage_class.as_deref(),
    );
    let db_path = Path::from(path_from_url.to_string());

    let mut admin_builder = AdminBuilder::new(db_path, object_store);
    if let Some(wal_config) = &settings.wal {
        admin_builder = admin_builder.with_wal_object_store(parse_wal_object_store(wal_config)?);
    }
    let admin = admin_builder.build();

    let checkpoints = admin
        .list_checkpoints(Some(name))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list checkpoints: {}", e))?;

    checkpoints
        .into_iter()
        .find(|cp| cp.name.as_deref() == Some(name))
        .map(|cp| cp.id)
        .ok_or_else(|| anyhow::anyhow!("Checkpoint '{}' not found", name))
}

async fn start_nfs_servers(
    fs: Arc<ZeroFS>,
    config: Option<&NfsConfig>,
    shutdown: CancellationToken,
) -> Vec<JoinHandle<Result<(), std::io::Error>>> {
    let config = match config {
        Some(c) => c,
        None => return Vec::new(),
    };
    let mut handles = Vec::new();

    if let Some(addresses) = &config.addresses {
        for addr in addresses {
            info!("Starting NFS server on {}", addr);
            let fs_clone = Arc::clone(&fs);
            let addr = *addr;
            let shutdown_clone = shutdown.clone();
            handles.push(spawn_named("nfs-server", async move {
                match crate::nfs::start_nfs_server_with_config(fs_clone, addr, shutdown_clone).await
                {
                    Ok(()) => Ok(()),
                    Err(e) => Err(std::io::Error::other(e.to_string())),
                }
            }));
        }
    }

    handles
}

fn start_ninep_servers(
    fs: Arc<ZeroFS>,
    config: Option<&NinePConfig>,
    shutdown: CancellationToken,
) -> Vec<JoinHandle<Result<(), std::io::Error>>> {
    let config = match config {
        Some(c) => c,
        None => return Vec::new(),
    };
    let mut handles = Vec::new();

    if let Some(addresses) = &config.addresses {
        for addr in addresses {
            info!("Starting 9P server on {}", addr);
            let ninep_tcp_server = crate::ninep::NinePServer::new(Arc::clone(&fs), *addr);
            let shutdown_clone = shutdown.clone();
            handles.push(spawn_named("9p-server", async move {
                ninep_tcp_server.start(shutdown_clone).await
            }));
        }
    }

    if let Some(socket_path) = config.unix_socket.as_ref() {
        info!(
            "Starting 9P server on Unix socket: {}",
            socket_path.display()
        );
        let ninep_unix_fs = Arc::clone(&fs);
        let ninep_unix_server =
            crate::ninep::NinePServer::new_unix(ninep_unix_fs, socket_path.clone());
        let shutdown_clone = shutdown.clone();
        handles.push(spawn_named("9p-unix-server", async move {
            ninep_unix_server.start(shutdown_clone).await
        }));
    }

    handles
}

async fn ensure_nbd_directory(fs: &Arc<ZeroFS>) -> Result<()> {
    let creds = Credentials {
        uid: 0,
        gid: 0,
        gid_known: true,
        groups: [0; 16],
        groups_count: 1,
        groups_complete: true,
    };
    let nbd_name = b".nbd";

    match fs.lookup(&creds, 0, nbd_name).await {
        Ok(_) => info!(".nbd directory already exists"),
        Err(e) => {
            debug!(".nbd directory lookup returned: {:?}, will create it", e);
            let attr = SetAttributes {
                mode: crate::fs::types::SetMode::Set(0o755),
                uid: crate::fs::types::SetUid::Set(0),
                gid: crate::fs::types::SetGid::Set(0),
                ..Default::default()
            };
            fs.mkdir(&creds, 0, nbd_name, &attr)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create .nbd directory: {e:?}"))?;
            info!("Created .nbd directory for NBD device management");
        }
    }
    Ok(())
}

async fn start_nbd_servers(
    fs: Arc<ZeroFS>,
    config: Option<&NbdConfig>,
    shutdown: CancellationToken,
) -> Vec<JoinHandle<Result<(), std::io::Error>>> {
    let config = match config {
        Some(c) => c,
        None => return Vec::new(),
    };
    let mut handles = Vec::new();

    if let Some(addresses) = &config.addresses {
        for addr in addresses {
            info!(
                "Starting NBD server on {} (devices dynamically discovered from .nbd/)",
                addr
            );
            let nbd_tcp_server = NBDServer::new_tcp(Arc::clone(&fs), *addr);
            let shutdown_clone = shutdown.clone();
            handles.push(spawn_named("nbd-server", async move {
                if let Err(e) = nbd_tcp_server.start(shutdown_clone).await {
                    Err(e)
                } else {
                    Ok(())
                }
            }));
        }
    }

    if let Some(socket_path) = config.unix_socket.as_ref() {
        info!(
            "Starting NBD server on Unix socket {} (devices dynamically discovered from .nbd/)",
            socket_path.display()
        );
        let nbd_unix_server = NBDServer::new_unix(Arc::clone(&fs), socket_path);
        let shutdown_clone = shutdown.clone();
        handles.push(spawn_named("nbd-unix-server", async move {
            if let Err(e) = nbd_unix_server.start(shutdown_clone).await {
                Err(e)
            } else {
                Ok(())
            }
        }));
    }

    handles
}

async fn start_rpc_servers(
    config: Option<&RpcConfig>,
    checkpoint_manager: Arc<CheckpointManager>,
    checkpoint_catalog: Option<Arc<dyn crate::rpc::server::CheckpointCatalogAuthority>>,
    catalog_configured: bool,
    customer_catalog: Option<zerofs::catalog::CustomerCatalog>,
    fs: Arc<ZeroFS>,
    shutdown: CancellationToken,
) -> Vec<JoinHandle<Result<(), std::io::Error>>> {
    let config = match config {
        Some(c) => c,
        None => return Vec::new(),
    };

    let service = crate::rpc::server::AdminRpcServer::new_with_catalog(
        checkpoint_manager,
        checkpoint_catalog,
        catalog_configured,
        customer_catalog,
        fs,
        shutdown.clone(),
    );
    let mut handles = Vec::new();

    if let Some(addresses) = &config.addresses {
        for &addr in addresses {
            info!("Starting RPC server on {}", addr);
            let service = service.clone();
            let shutdown_clone = shutdown.clone();
            handles.push(spawn_named("rpc-server", async move {
                crate::rpc::server::serve_tcp(addr, service, shutdown_clone)
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))
            }));
        }
    }

    if let Some(socket_path) = &config.unix_socket {
        info!(
            "Starting RPC server on Unix socket: {}",
            socket_path.display()
        );
        let socket_path = socket_path.clone();
        let service = service.clone();
        let shutdown_clone = shutdown.clone();
        handles.push(spawn_named("rpc-unix-server", async move {
            crate::rpc::server::serve_unix(socket_path, service, shutdown_clone)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))
        }));
    }

    handles
}

fn start_stats_reporting(fs: Arc<ZeroFS>, shutdown: CancellationToken) -> JoinHandle<()> {
    spawn_named("stats-reporting", async move {
        info!("Starting stats reporting task (reports to debug every 5 seconds)");
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Stats reporting task shutting down");
                    break;
                }
                _ = interval.tick() => {
                    fs.stats.output_report_debug();
                }
            }
        }
    })
}

fn start_periodic_flush(
    fs: Arc<ZeroFS>,
    interval_secs: u64,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    spawn_named("periodic-flush", async move {
        info!(
            "Starting periodic flush task (flushes every {} seconds)",
            interval_secs
        );
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    info!("Periodic flush task shutting down");
                    break;
                }
                _ = interval.tick() => {
                    if let Err(e) = fs.flush_coordinator.flush().await {
                        tracing::error!("Periodic flush failed: {:?}", e);
                    }
                }
            }
        }
    })
}

fn leadership_lost_error() -> anyhow::Error {
    anyhow::anyhow!("HA writer was fenced or superseded; restart required")
}

/// Walk an error's source chain looking for an open-file-descriptor exhaustion
/// (EMFILE/ENFILE). foyer reports these as an opaque `I/O error => coding error`
/// whose only clue is the wrapped os error code, so detection has to go by the
/// raw code rather than the (libc-dependent) message text.
fn is_fd_exhaustion(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut source = Some(err);
    while let Some(e) = source {
        if let Some(io) = e.downcast_ref::<std::io::Error>()
            && matches!(io.raw_os_error(), Some(libc::EMFILE) | Some(libc::ENFILE))
        {
            return true;
        }
        source = e.source();
    }
    false
}

/// Wrap a foyer cache build failure, keeping the real error visible and adding a
/// `ulimit -n` hint when the cause is fd exhaustion (which foyer otherwise hides
/// behind a useless "coding error").
fn foyer_build_error(context: &str, err: foyer::Error) -> anyhow::Error {
    if is_fd_exhaustion(&err) {
        anyhow::anyhow!(
            "{context}: {err}\n\nZeroFS ran out of open file descriptors while building \
             the on-disk cache. Raise the open-file limit (e.g. `ulimit -n 1048576`, or \
             LimitNOFILE= in the systemd unit) and restart."
        )
    } else {
        anyhow::anyhow!("{context}: {err}")
    }
}

/// Build the foyer hybrid cache used as slatedb's block cache. Shared by the
/// server open path and the warm-metadata integration test.
pub(crate) async fn build_block_hybrid(
    hybrid_cache_root: &std::path::Path,
    memory_bytes: usize,
    disk_bytes: usize,
    foyer_handle: &tokio::runtime::Handle,
) -> Result<Arc<FoyerHybridCache>> {
    tokio::fs::create_dir_all(hybrid_cache_root)
        .await
        .with_context(|| {
            format!(
                "creating foyer hybrid cache dir at {}",
                hybrid_cache_root.display()
            )
        })?;

    let hybrid = HybridCacheBuilder::new()
        .with_name("zerofs-slatedb-hybrid")
        .memory(memory_bytes)
        .with_eviction_config(S3FifoConfig::default())
        .with_weighter(|_, v: &slatedb::db_cache::CachedEntry| v.size())
        .storage()
        .with_spawner(Spawner::from(foyer_handle.clone()))
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_engine_config(
            BlockEngineConfig::new(
                FsDeviceBuilder::new(hybrid_cache_root)
                    .with_capacity(disk_bytes)
                    .build()
                    .map_err(|e| foyer_build_error("foyer device build failed", e))?,
            )
            .with_block_size(64 * 1024 * 1024),
        )
        .build()
        .await
        .map_err(|e| foyer_build_error("foyer hybrid build failed", e))?;
    Ok(Arc::new(FoyerHybridCache::new_with_cache(hybrid)))
}

/// Block size of the parts disk cache: foyer's eviction/reclaim unit, and the
/// max cacheable entry size.
const PARTS_BLOCK_SIZE: usize = 64 * 1024 * 1024;

/// Disk-engine knobs for the parts cache, scaled to the device.
struct PartsEngineKnobs {
    flushers: usize,
    clean_block_threshold: usize,
    submit_queue_bytes: usize,
    buffer_pool_bytes: usize,
}

fn parts_engine_knobs(disk_bytes: usize) -> PartsEngineKnobs {
    let blocks = disk_bytes / PARTS_BLOCK_SIZE;
    let flushers = (blocks / 8).clamp(1, 4);

    PartsEngineKnobs {
        flushers,
        clean_block_threshold: (blocks / 64).clamp(flushers, 8),
        submit_queue_bytes: (disk_bytes / 4).clamp(16 * 1024 * 1024, 1024 * 1024 * 1024),
        buffer_pool_bytes: flushers * PARTS_BLOCK_SIZE,
    }
}

pub(crate) async fn build_parts_hybrid(
    cache_root: &std::path::Path,
    memory_bytes: usize,
    disk_bytes: usize,
    foyer_handle: &tokio::runtime::Handle,
) -> Result<foyer::HybridCache<crate::object_store_prefetch::PartKey, bytes::Bytes>> {
    use crate::object_store_prefetch::PartKey;
    use bytes::Bytes;

    let parts_root = cache_root.join("parts_cache");
    tokio::fs::create_dir_all(&parts_root)
        .await
        .with_context(|| format!("creating parts cache dir at {}", parts_root.display()))?;

    let knobs = parts_engine_knobs(disk_bytes);

    HybridCacheBuilder::new()
        .with_name("zerofs-object-prefetch-parts")
        .memory(memory_bytes)
        .with_eviction_config(S3FifoConfig::default())
        .with_weighter(|_: &PartKey, v: &Bytes| v.len())
        .storage()
        .with_spawner(Spawner::from(foyer_handle.clone()))
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_engine_config(
            BlockEngineConfig::new(
                FsDeviceBuilder::new(&parts_root)
                    .with_capacity(disk_bytes)
                    .build()
                    .map_err(|e| foyer_build_error("parts foyer device build failed", e))?,
            )
            .with_block_size(PARTS_BLOCK_SIZE)
            .with_submit_queue_size_threshold(knobs.submit_queue_bytes)
            .with_flushers(knobs.flushers)
            .with_reclaimers(knobs.flushers)
            .with_clean_block_threshold(knobs.clean_block_threshold)
            .with_buffer_pool_size(knobs.buffer_pool_bytes),
        )
        .build()
        .await
        .map_err(|e| foyer_build_error("parts foyer hybrid build failed", e))
}

/// Split the configured disk-cache total into
/// (parts_disk_bytes, decoded_blocks_disk_bytes).
///
/// SlateDB holds only metadata — a small working set the raw-parts cache backs
/// anyway (it caches SST object bytes next to segment bytes, so a decoded-cache
/// miss is a parts-cache hit plus a re-decode). The decoded-blocks side gets a
/// bounded slice; the parts cache, where the bulk segment bytes live, gets the
/// rest. Floors keep either side from collapsing on a tiny config.
pub(crate) fn split_disk_budget(total_disk_bytes: usize) -> (usize, usize) {
    const MIN_BYTES: usize = 1024 * 1024 * 1024; // 1 GiB floor per side
    // u64: 16 GiB overflows usize on 32-bit targets
    const MAX_META_BYTES: u64 = 16 * 1024 * 1024 * 1024; // metadata rarely needs more

    let max_meta = usize::try_from(MAX_META_BYTES).unwrap_or(usize::MAX);
    let decoded = (total_disk_bytes / 10).clamp(MIN_BYTES, max_meta);
    let parts = total_disk_bytes.saturating_sub(decoded).max(MIN_BYTES);
    (parts, decoded)
}

/// Split the configured memory-cache total into (parts_memory_bytes,
/// decoded_blocks_memory_bytes). Same data-favored split as
/// [`split_disk_budget`], with memory-scale floors so a small default still
/// splits.
pub(crate) fn split_memory_budget(total_memory_bytes: usize) -> (usize, usize) {
    const MIN_BYTES: usize = 32 * 1024 * 1024; // 32 MiB floor per side
    const MAX_META_BYTES: usize = 2 * 1024 * 1024 * 1024; // metadata blocks rarely need more

    let decoded = (total_memory_bytes / 4).clamp(MIN_BYTES, MAX_META_BYTES);
    let parts = total_memory_bytes.saturating_sub(decoded).max(MIN_BYTES);
    (parts, decoded)
}

/// Result of opening the ZeroFS database.
pub struct SlateDbOpen {
    pub data: SlateDbHandle,
    pub metrics_recorder: Option<Arc<DefaultMetricsRecorder>>,
    /// The raw-parts prefetch cache, returned so the segment store reuses it
    /// (one budget; segment objects and SST objects share it, keyed by path).
    pub parts_cache: foyer::HybridCache<crate::object_store_prefetch::PartKey, bytes::Bytes>,
}

/// Process-wide runtime for cache, database, and GC maintenance.
fn shared_maintenance_runtime() -> &'static tokio::runtime::Handle {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("zerofs-maintenance")
                .build()
                .expect("failed to build maintenance runtime")
        })
        .handle()
}

// SlateDB 0.15 validates that max_unflushed_bytes is strictly greater than
// l0_sst_size_bytes. ZeroFS must effectively disable both thresholds because
// only a seal-barrier-controlled flush may make metadata durable.
const BARRIER_CONTROLLED_L0_SST_SIZE_BYTES: usize = usize::MAX - 1;
const BARRIER_CONTROLLED_MAX_UNFLUSHED_BYTES: usize = usize::MAX;

#[allow(clippy::too_many_arguments)]
pub async fn build_slatedb(
    object_store: Arc<dyn object_store::ObjectStore>,
    cache_config: &CacheConfig,
    db_path: String,
    db_mode: DatabaseMode,
    lsm_config: Option<crate::config::LsmConfig>,
    block_transformer: Arc<dyn BlockTransformer>,
    wal_object_store: Option<Arc<dyn object_store::ObjectStore>>,
    replication: Option<&crate::replication::ReplicationParams>,
) -> Result<SlateDbOpen> {
    let total_disk_cache_gb = cache_config.max_cache_size_gb;
    let total_memory_cache_gb = cache_config.memory_cache_size_gb.unwrap_or(0.25);

    let total_disk_bytes = (total_disk_cache_gb * 1_000_000_000.0) as usize;
    let (parts_disk_bytes, hybrid_disk_bytes) = split_disk_budget(total_disk_bytes);
    let total_memory_bytes = (total_memory_cache_gb * 1_000_000_000.0) as usize;
    let (parts_memory_bytes, hybrid_memory_bytes) = split_memory_budget(total_memory_bytes);

    info!(
        "Cache allocation - Disk: {:.2}GB total ({} MB decoded-blocks + {} MB raw-parts), \
         Memory: {:.2}GB total ({} MB decoded-blocks + {} MB raw-parts)",
        total_disk_cache_gb,
        hybrid_disk_bytes / 1_000_000,
        parts_disk_bytes / 1_000_000,
        total_memory_cache_gb,
        hybrid_memory_bytes / 1_000_000,
        parts_memory_bytes / 1_000_000,
    );

    let l0_max_ssts = lsm_config
        .map(|c| c.l0_max_ssts())
        .unwrap_or(crate::config::LsmConfig::DEFAULT_L0_MAX_SSTS);
    let max_concurrent_compactions = lsm_config
        .map(|c| c.max_concurrent_compactions())
        .unwrap_or(crate::config::LsmConfig::DEFAULT_MAX_CONCURRENT_COMPACTIONS);

    // Replication needs the writer path: reject read-only / checkpoint, and
    // reject reaching here as a standby (a standby opens the data db as writer
    // only on promotion; doing so here would fence the live leader).
    if let Some(repl) = replication {
        if db_mode.is_read_only() {
            anyhow::bail!(
                "[replication] is incompatible with read-only / checkpoint database modes; \
                 node {} must open the data database as a writer",
                repl.node_id
            );
        }
        if !repl.is_leader() {
            anyhow::bail!(
                "internal error: build_slatedb reached as a standby (node {}); a standby must \
                 complete failover and be promoted to leader before opening the data database",
                repl.node_id
            );
        }
    }

    // The WAL is permanently off, a correctness requirement: with it on,
    // SlateDB flushes durably on the write path without taking our seal
    // barrier, so a FrameLoc could become durable while its segment is still
    // the un-PUT open buffer (a dangling pointer after a crash). With it off,
    // the barrier-gated flush — which seals the open segment first — is the
    // only path that makes metadata durable.
    let wal_enabled = false;

    let settings = slatedb::config::Settings {
        wal_enabled,
        l0_max_ssts,
        l0_max_ssts_per_key: l0_max_ssts,
        // Disable SlateDB's write-path memtable size-freeze (`flush_interval:
        // None` does not — that only kills the WAL timer). Left finite, the
        // size check would dispatch a durable L0 flush from a background task
        // that never takes our seal barrier, publishing FrameLocs for a
        // still-un-PUT segment. Keep both size thresholds effectively disabled
        // so the memtable freezes only on our barrier-gated `db.flush()`, which
        // also drains it (RAM-bounded) on every flush. SlateDB requires the
        // backpressure threshold to be strictly greater than the freeze
        // threshold, hence MAX - 1 and MAX rather than MAX for both.
        l0_sst_size_bytes: BARRIER_CONTROLLED_L0_SST_SIZE_BYTES,
        compactor_options: None,
        flush_interval: None,
        // Independent of HA authority checks.
        manifest_poll_interval: std::time::Duration::from_secs(5),
        max_unflushed_bytes: BARRIER_CONTROLLED_MAX_UNFLUSHED_BYTES,
        compression_codec: None, // Disable compression as we handle it in encryption layer
        l0_flush_parallelism: 16,
        min_filter_keys: 10,
        garbage_collector_options: Some(GarbageCollectorOptions {
            wal_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_mins(1)),
                min_age: Duration::from_mins(1),
                dry_run: false,
            }),
            manifest_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_mins(1)),
                min_age: Duration::from_mins(1),
                dry_run: false,
            }),
            compacted_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_mins(1)),
                min_age: Duration::from_mins(1),
                dry_run: false,
            }),
            compactions_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_mins(1)),
                min_age: Duration::from_mins(1),
                dry_run: false,
            }),
            detach_options: None,
            // Disable WAL fence GC: it defaults to a dry-run that does nothing
            // but logs a conservative-setting warning every interval. See #352.
            wal_fence_options: None,
            ..Default::default()
        }),
        ..Default::default()
    };

    // HA startup retries share the process-wide maintenance runtime.
    let maintenance_runtime = shared_maintenance_runtime().clone();

    let hybrid_cache_root = cache_config.root_folder.join("hybrid_cache");
    let cache = build_block_hybrid(
        &hybrid_cache_root,
        hybrid_memory_bytes,
        hybrid_disk_bytes,
        &maintenance_runtime,
    )
    .await?;

    let parts_cache = build_parts_hybrid(
        &cache_config.root_folder,
        parts_memory_bytes,
        parts_disk_bytes,
        &maintenance_runtime,
    )
    .await?;

    // Length-check the store before the data-db prefetch wrapper is layered on;
    // the compactor uses the length-checked store directly (no prefetch cache).
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LengthCheckedObjectStore::new(object_store));
    let compactor_object_store = object_store.clone();
    let wal_object_store = wal_object_store
        .map(|s| Arc::new(LengthCheckedObjectStore::new(s)) as Arc<dyn object_store::ObjectStore>);
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(PrefetchingObjectStore::new(
        object_store,
        parts_cache.clone(),
    ));

    let db_path = Path::from(db_path);

    match db_mode {
        DatabaseMode::ReadWrite => {
            info!("Opening database in read-write mode");

            let metrics_recorder = Arc::new(DefaultMetricsRecorder::new());

            let mut builder = DbBuilder::new(db_path.clone(), object_store.clone())
                .with_settings(settings)
                .with_gc_runtime(maintenance_runtime.clone())
                .with_sst_block_size(slatedb::SstBlockSize::Block32Kib)
                .with_db_cache(cache)
                .with_block_transformer(block_transformer)
                .with_filter_policies(crate::fs::filter_policy::filter_policies())
                .with_metrics_recorder(metrics_recorder.clone())
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor));

            if let Some(wal_store) = wal_object_store {
                builder = builder.with_wal_object_store(wal_store);
            }

            // The compaction coordinator is bound to the read-write DB, so it
            // runs only on the current leader. SlateDB holds only metadata, so
            // its compaction is light enough to embed in-process.
            {
                let scheduler_options: std::collections::HashMap<String, String> =
                    slatedb::config::SizeTieredCompactionSchedulerOptions {
                        max_compaction_sources: 16,
                        ..Default::default()
                    }
                    .into();
                let worker = Some(slatedb::config::CompactionWorkerOptions {
                    max_sst_size: 256 * 1024 * 1024,
                    max_fetch_tasks: 2,
                    bytes_to_fetch: 8 * 1024 * 1024,
                    // Metadata-only DB now that chunks live outside SlateDB, so
                    // compactions are small. Match the 2-job coordinator cap: the
                    // 256MiB max_sst_size floor keeps a compaction single-range
                    // until its input tops 512MiB, so only a rare large one splits
                    // into a second sub-range instead of running single-threaded.
                    max_subcompactions: 2,
                    ..Default::default()
                });
                let compactor = CompactorBuilder::new(db_path, compactor_object_store)
                    .with_runtime(maintenance_runtime.clone())
                    .with_filter_policies(crate::fs::filter_policy::filter_policies())
                    .with_options(slatedb::config::CompactorOptions {
                        poll_interval: std::time::Duration::from_secs(5),
                        commit_compacted_interval: std::time::Duration::from_secs(5),
                        max_concurrent_compactions,
                        scheduler_options,
                        worker,
                        ..Default::default()
                    });

                builder = builder.with_compactor_builder(compactor);
            }

            let slatedb = Arc::new(
                builder
                    .build()
                    .await
                    .context("Failed to build SlateDB instance")?,
            );

            Ok(SlateDbOpen {
                data: SlateDbHandle::ReadWrite(slatedb),
                metrics_recorder: Some(metrics_recorder),
                parts_cache: parts_cache.clone(),
            })
        }
        DatabaseMode::ReadOnly => {
            info!("Opening database in read-only mode");

            let mut reader_builder = DbReader::builder(db_path, object_store)
                .with_block_transformer(block_transformer)
                .with_filter_policies(crate::fs::filter_policy::filter_policies())
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor));
            if let Some(wal_store) = wal_object_store {
                reader_builder = reader_builder.with_wal_object_store(wal_store);
            }
            let reader = Arc::new(
                reader_builder
                    .build()
                    .await
                    .context("Failed to open database in read-only mode")?,
            );

            Ok(SlateDbOpen {
                data: SlateDbHandle::ReadOnly(ArcSwap::new(reader)),
                metrics_recorder: None,
                parts_cache: parts_cache.clone(),
            })
        }
        DatabaseMode::Checkpoint(checkpoint_id) => {
            info!("Opening database from checkpoint ID: {}", checkpoint_id);

            let mut reader_builder = DbReader::builder(db_path, object_store)
                .with_reader_mode(DbReaderMode::Checkpoint(checkpoint_id))
                .with_block_transformer(block_transformer)
                .with_filter_policies(crate::fs::filter_policy::filter_policies())
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor));
            if let Some(wal_store) = wal_object_store {
                reader_builder = reader_builder.with_wal_object_store(wal_store);
            }
            let reader = Arc::new(
                reader_builder
                    .build()
                    .await
                    .context("Failed to open database from checkpoint")?,
            );

            Ok(SlateDbOpen {
                data: SlateDbHandle::ReadOnly(ArcSwap::new(reader)),
                metrics_recorder: None,
                parts_cache: parts_cache.clone(),
            })
        }
    }
}

pub struct InitResult {
    pub fs: Arc<ZeroFS>,
    pub object_store: Arc<dyn object_store::ObjectStore>,
    pub wal_object_store: Option<Arc<dyn object_store::ObjectStore>>,
    pub db_path: String,
    pub db_handle: SlateDbHandle,
    /// HA authority monitors retained through database close.
    pub authority: Option<crate::replication::AuthoritySupervisor>,
    /// Retains the authoritative catalog lifecycle for the serving process;
    /// lifecycle APIs and stable mount wiring consume it incrementally.
    pub(crate) catalog_runtime: Option<crate::cli::init::CatalogRuntime>,
    pub(crate) branch_mount: Option<crate::cli::init::ConfiguredBranchMount>,
}

async fn close_catalog_runtime(
    runtime: Option<&crate::cli::init::CatalogRuntime>,
) -> anyhow::Result<()> {
    if let Some(runtime) = runtime {
        runtime.close().await?;
    }
    Ok(())
}

struct BranchWriterLeaseSupervisor {
    grant: Arc<tokio::sync::Mutex<zerofs::catalog::LeaseGrant>>,
    stop: CancellationToken,
    lost: CancellationToken,
    task: JoinHandle<()>,
    duration: chrono::Duration,
    reconciler: Option<BranchWriterReconciler>,
}

const BRANCH_RENEWAL_RECONCILE_TIMEOUT: Duration = Duration::from_secs(5);
const BRANCH_INITIAL_RENEWAL_TIMEOUT: Duration = Duration::from_secs(5);

type BranchWriterRenewer = Arc<
    dyn Fn(
            zerofs::catalog::LeaseGrant,
            chrono::Duration,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = std::result::Result<
                            zerofs::catalog::LeaseGrant,
                            BranchWriterRenewalFailure,
                        >,
                    > + Send,
            >,
        > + Send
        + Sync,
>;

struct BranchWriterRenewalFailure {
    latest: zerofs::catalog::LeaseGrant,
    error: anyhow::Error,
}

fn branch_writer_renewal_safety_margin(lease_duration_seconds: u64) -> Duration {
    Duration::from_millis((lease_duration_seconds.saturating_mul(100)).clamp(100, 30_000))
}

fn branch_writer_time_to_safety_deadline(
    grant: &zerofs::catalog::LeaseGrant,
    safety_margin: Duration,
) -> Result<Duration> {
    let renew_by = grant.lease.expires_at
        - chrono::Duration::from_std(safety_margin)
            .expect("bounded renewal safety margin fits chrono");
    (renew_by - chrono::Utc::now())
        .to_std()
        .ok()
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| anyhow::anyhow!("configured branch writer reached its renewal deadline"))
}

async fn confirm_branch_writer_before_serving<F>(
    current: &zerofs::catalog::LeaseGrant,
    safety_margin: Duration,
    attempt: F,
) -> Result<zerofs::catalog::LeaseGrant>
where
    F: std::future::Future<Output = Result<zerofs::catalog::LeaseGrant>>,
{
    let timeout = branch_writer_time_to_safety_deadline(current, safety_margin)?
        .min(BRANCH_INITIAL_RENEWAL_TIMEOUT);
    tokio::time::timeout(timeout, attempt)
        .await
        .context("Initial configured branch writer renewal timed out")?
}

async fn recover_branch_writer_bounded<F>(
    timeout: Duration,
    recovery: F,
) -> Result<zerofs::catalog::LeaseGrant>
where
    F: std::future::Future<Output = Result<zerofs::catalog::LeaseGrant>>,
{
    tokio::time::timeout(timeout, recovery)
        .await
        .context("Exact configured branch writer reconciliation timed out")?
}

type BranchWriterReconciler = Arc<
    dyn Fn(
            zerofs::catalog::LeaseGrant,
            chrono::Duration,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<zerofs::catalog::LeaseGrant>> + Send>,
        > + Send
        + Sync,
>;

impl BranchWriterLeaseSupervisor {
    async fn start(
        runtime: crate::cli::init::CatalogRuntime,
        config: crate::config::ServerBranchMountConfig,
        mount: zerofs::catalog::ServerWriterMountPreparation,
    ) -> Result<Self> {
        let lease_duration_seconds = config.lease_duration_seconds;
        let duration = chrono::Duration::seconds(lease_duration_seconds as i64);
        // Confirm and extend authority synchronously before any listener can
        // acknowledge a request. This also catches a lease that expired during
        // database and filesystem assembly.
        let initial = confirm_branch_writer_before_serving(
            &mount.grant,
            branch_writer_renewal_safety_margin(lease_duration_seconds),
            runtime.renew_writer_mount(&mount.grant, duration),
        )
        .await?;
        let reconcile_runtime = runtime.clone();
        let reconciler: BranchWriterReconciler = Arc::new(move |current, duration| {
            let runtime = reconcile_runtime.clone();
            Box::pin(async move { runtime.recover_writer_mount(&current, duration).await })
        });
        let renewer: BranchWriterRenewer = Arc::new(move |current, duration| {
            let runtime = runtime.clone();
            Box::pin(async move {
                match runtime.renew_writer_mount(&current, duration).await {
                    Ok(renewed) => Ok(renewed),
                    Err(error) => {
                        // Renewal may have committed before its response was
                        // lost. Recover the deterministic exact capability so
                        // shutdown publishes with the latest known revision.
                        let latest = match runtime.recover_writer_mount(&current, duration).await {
                            Ok(recovered) => recovered,
                            Err(reconcile_error) => {
                                tracing::warn!(
                                    "configured branch renewal failure could not reconcile the exact latest grant: {reconcile_error:#}"
                                );
                                current.clone()
                            }
                        };
                        Err(BranchWriterRenewalFailure { latest, error })
                    }
                }
            })
        });
        let mut supervisor =
            Self::spawn_after_confirmation(initial, lease_duration_seconds, renewer);
        supervisor.reconciler = Some(reconciler);
        Ok(supervisor)
    }

    fn spawn_after_confirmation(
        initial: zerofs::catalog::LeaseGrant,
        lease_duration_seconds: u64,
        renewer: BranchWriterRenewer,
    ) -> Self {
        let grant = Arc::new(tokio::sync::Mutex::new(initial));
        let stop = CancellationToken::new();
        let lost = CancellationToken::new();
        let task_grant = Arc::clone(&grant);
        let task_stop = stop.clone();
        let task_lost = lost.clone();
        let duration = chrono::Duration::seconds(lease_duration_seconds as i64);
        let interval =
            Duration::from_millis((lease_duration_seconds.saturating_mul(1_000) / 3).max(100));
        let safety_margin = branch_writer_renewal_safety_margin(lease_duration_seconds);
        let worker_stop = task_stop.clone();
        let worker_lost = task_lost.clone();
        let worker = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = worker_stop.cancelled() => return,
                    _ = tokio::time::sleep(interval) => {}
                }
                let current = task_grant.lock().await.clone();
                let remaining = match branch_writer_time_to_safety_deadline(&current, safety_margin)
                {
                    Ok(remaining) => remaining,
                    Err(_) => {
                        tracing::error!(
                            "configured branch writer lease reached its renewal safety deadline; stopping serving"
                        );
                        worker_lost.cancel();
                        return;
                    }
                };
                let outcome = tokio::select! {
                    biased;
                    _ = worker_stop.cancelled() => return,
                    outcome = tokio::time::timeout(remaining, renewer(current, duration)) => outcome,
                };
                match outcome {
                    Ok(Ok(renewed)) => *task_grant.lock().await = renewed,
                    Ok(Err(failure)) => {
                        *task_grant.lock().await = failure.latest;
                        tracing::error!(
                            "configured branch writer lease renewal failed; stopping serving: {:#}",
                            failure.error
                        );
                        worker_lost.cancel();
                        return;
                    }
                    Err(_) => {
                        tracing::error!(
                            "configured branch writer lease renewal did not finish before the safety deadline; stopping serving"
                        );
                        worker_lost.cancel();
                        return;
                    }
                }
            }
        });
        // A panic or any unexpected worker exit revokes serving. A clean stop
        // cancels the stop token before waiting for this monitor.
        let task = tokio::spawn(async move {
            match worker.await {
                Ok(()) if task_stop.is_cancelled() => {}
                Ok(()) => task_lost.cancel(),
                Err(error) => {
                    tracing::error!(
                        "configured branch writer renewal task exited unexpectedly; stopping serving: {error}"
                    );
                    task_lost.cancel();
                }
            }
        });
        Self {
            grant,
            stop,
            lost,
            task,
            duration,
            reconciler: None,
        }
    }

    fn loss_token(&self) -> CancellationToken {
        self.lost.clone()
    }

    async fn stop(self) -> zerofs::catalog::LeaseGrant {
        self.stop.cancel();
        let _ = self.task.await;
        let current = self.grant.lock().await.clone();
        let Some(reconciler) = self.reconciler else {
            return current;
        };
        match recover_branch_writer_bounded(
            BRANCH_RENEWAL_RECONCILE_TIMEOUT,
            reconciler(current.clone(), self.duration),
        )
        .await
        {
            Ok(recovered)
                if recovered.lease.id == current.lease.id
                    && recovered.renewal_token == current.renewal_token =>
            {
                recovered
            }
            Ok(_) => {
                tracing::warn!(
                    "exact branch writer reconciliation returned a different capability; retaining the prior grant"
                );
                current
            }
            Err(error) => {
                tracing::warn!(
                    "exact branch writer reconciliation before publication failed: {error:#}"
                );
                current
            }
        }
    }
}

async fn close_unserved_filesystem(
    fs: &ZeroFS,
    db_mode: DatabaseMode,
    catalog_runtime: Option<&crate::cli::init::CatalogRuntime>,
    branch_writer: Option<BranchWriterLeaseSupervisor>,
    branch_grant: Option<zerofs::catalog::LeaseGrant>,
) -> Result<()> {
    let branch_grant = match (branch_writer, branch_grant) {
        (Some(writer), None) => Some(writer.stop().await),
        (None, grant) => grant,
        (Some(_), Some(_)) => anyhow::bail!("duplicate branch writer close authority"),
    };
    let data_close = if db_mode.is_read_only() {
        fs.db.close().await
    } else {
        fs.flush_coordinator
            .close()
            .await
            .map_err(anyhow::Error::from)
    };
    if let Err(error) = data_close {
        let _ = close_catalog_runtime(catalog_runtime).await;
        return Err(error).context("Failed to close initialized but unserved database");
    }
    let publication_result = match (catalog_runtime, branch_grant.as_ref()) {
        (Some(runtime), Some(grant)) => runtime.publish_writer_head(grant).await,
        _ => Ok(()),
    };
    let catalog_close_result = close_catalog_runtime(catalog_runtime).await;
    if let Err(publication_error) = publication_result {
        if let Err(catalog_error) = catalog_close_result {
            tracing::warn!(
                "authoritative catalog close also failed after writer-head publication error: {catalog_error:#}"
            );
        }
        return Err(publication_error);
    }
    catalog_close_result
}

const STARTUP_BANNER: &str = r#"
⠀⠀⠀⠀⠀⣠⣴⣶⣿⣿⣿⣿⣿⣷⣶⣤⣄
⠀⠀⢀⣴⣿⣿⣿⠿⠛⠛⠋⠉⠙⠻⠿⣿⣿⣿⣦⡀
⠀⣠⣿⣿⡿⠋⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⢿⣿⣿⡄
⢰⣿⣿⡟⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢿⣿⣿⡄⠀⠀⠀⢸⣿⣿⣿⣿⣿⣿⣿⡿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⠀⠀⢠⣶⣿⣿⣿⣿⣶⡆
⣾⣿⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠸⣿⣿⣷⠀⠀⠀⠀⠀⠀⠀⣠⣾⣿⠟⠁⠀⠀⣠⣴⣶⣶⣶⣤⡀⠀⠀⣶⣶⣆⣤⣶⣶⠀⢀⣤⣶⣶⣶⣦⣄⠀⠀⠀⣿⣿⡇⠀⠀⠀⠀⠀⠀⣿⣿⣏⠀⠀⠈⠉⠃
⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣿⣿⣿⠀⠀⠀⠀⠀⢀⣼⣿⡟⠁⠀⠀⠀⣼⣿⣟⣁⣀⣙⣿⣿⡀⠀⣿⣿⣿⠋⠉⠙⢠⣿⣿⠏⠀⠈⢻⣿⣧⠀⠀⣿⣿⣿⣿⣿⣿⡇⠀⠀⠘⠻⠿⣿⣿⣶⣦⣄
⢿⢿⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣿⣿⡿⠀⠀⠀⢀⣴⣿⡿⠋⠀⠀⠀⠀⠀⢿⣿⣟⠛⠛⠛⠛⠛⠃⠀⣿⣿⡇⠀⠀⠀⠸⣿⣿⡄⠀⠀⣸⣿⡿⠀⠀⣿⣿⡇⠀⠀⠀⠀⠀⠀⣄⣀⠀⠀⠀⣹⣿⣿
⠈⠈⢿⣇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣾⣿⣿⠃⠀⠀⠀⣾⣿⣿⣿⣿⣿⣿⣿⣿⠀⠈⠻⢿⣷⣶⣶⣶⠿⠀⠀⣿⣿⡇⠀⠀⠀⠀⠙⠿⣿⣶⣾⡿⠟⠁⠀⠀⣿⣿⡇⠀⠀⠀⠀⠀⠀⠻⠿⣿⣿⣿⣿⠿⠋
⠀⠀⠀⠻⣷⣦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢐⣽⣿⣿⠋
⠀⠀⠀⠀⠙⢿⣿⣶⣤⣀⣀⣀⣀⣤⣤⣶⣿⣿⠟⠁
⠀⠀⠀⠀⠀⠀⠉⠛⠿⢿⣿⣿⣿⣿⠿⠟⠋
"#;

pub async fn run_server(
    config_path: PathBuf,
    read_only: bool,
    checkpoint_name: Option<String>,
) -> Result<()> {
    use tracing_subscriber::EnvFilter;

    eprintln!("{STARTUP_BANNER}");

    // Default: ZeroFS at info, the embedded LSM engine at warn and above (the
    // metadata-compaction digest task summarizes its routine activity).
    // RUST_LOG replaces this entirely.
    let filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info,slatedb=warn"));

    #[cfg(feature = "tokio-console")]
    {
        use tracing_subscriber::prelude::*;
        let console_layer = console_subscriber::spawn();
        tracing_subscriber::registry()
            .with(console_layer)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_filter(filter),
            )
            .init();
    }

    #[cfg(not(feature = "tokio-console"))]
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    info!("ZeroFS v{}", env!("CARGO_PKG_VERSION"));

    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let db_mode = match (read_only, &checkpoint_name) {
        (false, None) => DatabaseMode::ReadWrite,
        (true, None) => DatabaseMode::ReadOnly,
        (false, Some(name)) => {
            let uuid = resolve_checkpoint_name(&settings, name)
                .await
                .with_context(|| format!("Failed to resolve checkpoint '{}'", name))?;
            DatabaseMode::Checkpoint(uuid)
        }
        (true, Some(_)) => {
            return Err(anyhow::anyhow!(
                "Cannot specify both --read-only and --checkpoint flags"
            ));
        }
    };
    let maintenance_runtime = if db_mode.is_read_only() {
        None
    } else {
        Some(shared_maintenance_runtime().clone())
    };

    let any_server = settings.servers.nfs.is_some()
        || settings.servers.ninep.is_some()
        || settings.servers.nbd.is_some()
        || settings.servers.rpc.is_some()
        || (cfg!(feature = "webui") && settings.servers.webui.is_some());
    if !any_server {
        anyhow::bail!(
            "No servers configured. At least one server (NFS, 9P, NBD, RPC, or enabled WebUI) must be enabled."
        );
    }

    // Register fallible process-signal state before opening either SlateDB
    // database so an unsupported/broken signal backend cannot strand a writer.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    crate::telemetry::send_startup_event(&settings);

    let init_result = crate::cli::init::initialize_filesystem(&settings, db_mode).await?;
    let fs = init_result.fs;
    let authority = init_result.authority;
    let catalog_runtime = init_result.catalog_runtime;
    let mut branch_writer = match (catalog_runtime.as_ref(), init_result.branch_mount) {
        (Some(runtime), Some(mount)) => {
            let fallback_grant = mount.preparation.grant.clone();
            match BranchWriterLeaseSupervisor::start(
                runtime.clone(),
                mount.config.clone(),
                mount.preparation,
            )
            .await
            {
                Ok(writer) => Some(writer),
                Err(error) => {
                    let duration =
                        chrono::Duration::seconds(mount.config.lease_duration_seconds as i64);
                    let current_grant = match recover_branch_writer_bounded(
                        BRANCH_RENEWAL_RECONCILE_TIMEOUT,
                        runtime.recover_writer_mount(&fallback_grant, duration),
                    )
                    .await
                    {
                        Ok(recovered) => recovered,
                        Err(reconcile_error) => {
                            tracing::warn!(
                                "initial branch renewal failure could not reconcile the exact grant before close: {reconcile_error:#}"
                            );
                            fallback_grant
                        }
                    };
                    close_unserved_filesystem(
                        &fs,
                        db_mode,
                        Some(runtime),
                        None,
                        Some(current_grant),
                    )
                    .await?;
                    return Err(error).context(
                        "Configured branch writer authority could not be confirmed before serving",
                    );
                }
            }
        }
        (None, None) | (Some(_), None) => None,
        (None, Some(_)) => anyhow::bail!("configured branch mount lost its catalog runtime"),
    };
    let leadership_deposed = authority
        .as_ref()
        .map_or_else(CancellationToken::new, |authority| authority.loss_token());
    let shutdown = leadership_deposed.child_token();
    let branch_lease_lost = branch_writer
        .as_ref()
        .map_or_else(CancellationToken::new, |writer| writer.loss_token());
    if branch_writer.is_some() {
        let loss = branch_lease_lost.clone();
        let serving_shutdown = shutdown.clone();
        tokio::spawn(async move {
            loss.cancelled().await;
            serving_shutdown.cancel();
        });
    }

    // Do not start listeners after authority was revoked during initialization.
    if leadership_deposed.is_cancelled() {
        return Err(leadership_lost_error());
    }

    if !db_mode.is_read_only()
        && settings.servers.nbd.is_some()
        && let Err(error) = ensure_nbd_directory(&fs).await
    {
        close_unserved_filesystem(
            &fs,
            db_mode,
            catalog_runtime.as_ref(),
            branch_writer.take(),
            None,
        )
        .await?;
        return Err(error);
    }

    let telemetry_handle = crate::telemetry::start_periodic_reporting(
        &settings,
        Arc::clone(&fs.global_stats),
        shutdown.clone(),
    );

    let prometheus_handles = if let Some(ref prometheus_config) = settings.prometheus {
        let slatedb_registry = fs.db.slatedb_metrics();
        crate::prometheus::start(
            prometheus_config,
            Arc::clone(&fs.stats),
            Arc::clone(&fs.global_stats),
            fs.extent_store.segment_gc_stats(),
            Arc::clone(&fs.dedup),
            slatedb_registry,
            shutdown.clone(),
        )
    } else {
        Vec::new()
    };

    // Metadata compaction digest: at most one line per interval, only when
    // compaction ran, plus a crossing-only L0 backlog warning. Summarizes the
    // engine's per-compaction lines, which the default filter drops.
    // Read-write mode only: readers run no compaction.
    let digest_handle = match (fs.db.slatedb_metrics(), fs.db.subscribe_status()) {
        (Some(recorder), Some(status)) => Some(crate::metadata_digest::spawn(
            recorder,
            status,
            settings
                .lsm
                .map(|c| c.l0_max_ssts())
                .unwrap_or(crate::config::LsmConfig::DEFAULT_L0_MAX_SSTS),
            shutdown.clone(),
        )),
        _ => None,
    };

    let nfs_handles = start_nfs_servers(
        Arc::clone(&fs),
        settings.servers.nfs.as_ref(),
        shutdown.clone(),
    )
    .await;

    let ninep_handles = start_ninep_servers(
        Arc::clone(&fs),
        settings.servers.ninep.as_ref(),
        shutdown.clone(),
    );

    let nbd_handles = start_nbd_servers(
        Arc::clone(&fs),
        settings.servers.nbd.as_ref(),
        shutdown.clone(),
    )
    .await;

    // A read-only admin over the same store for the GC's checkpoint gate; built
    // before the store/path are moved into the checkpoint manager below.
    let gc_admin = if !db_mode.is_read_only() {
        Some(
            AdminBuilder::new(
                slatedb::object_store::path::Path::from(init_result.db_path.clone()),
                Arc::clone(&init_result.object_store),
            )
            .build(),
        )
    } else {
        None
    };

    let checkpoint_manager = Arc::new(CheckpointManager::new(
        init_result.db_handle,
        slatedb::object_store::path::Path::from(init_result.db_path),
        init_result.object_store,
        init_result.wal_object_store.clone(),
    ));
    // Checkpoints must not durably publish a FrameLoc whose segment is still in
    // the RAM open buffer: seal + flush under the barrier first (see
    // CheckpointManager::create_checkpoint). Read-only mode has no writer to seal.
    if !db_mode.is_read_only() {
        let fc = fs.flush_coordinator.clone();
        checkpoint_manager.set_pre_flush(Arc::new(move || {
            let fc = fc.clone();
            Box::pin(async move {
                fc.flush()
                    .await
                    .map_err(|e| anyhow::anyhow!("seal+flush failed: {:?}", e))
            })
        }));
    }
    #[cfg(feature = "webui")]
    let checkpoint_manager_for_webui = Arc::clone(&checkpoint_manager);
    let checkpoint_catalog = catalog_runtime.as_ref().and_then(|runtime| {
        settings
            .catalog
            .as_ref()
            .and_then(|catalog| catalog.mount.as_ref())
            .map(|mount| {
                Arc::new(runtime.checkpoint_catalog(mount.expected_branch_id))
                    as Arc<dyn crate::rpc::server::CheckpointCatalogAuthority>
            })
    });
    let catalog_configured = settings.catalog.is_some();
    #[cfg(feature = "webui")]
    let checkpoint_catalog_for_webui = checkpoint_catalog.clone();
    let rpc_handles = start_rpc_servers(
        settings.servers.rpc.as_ref(),
        checkpoint_manager,
        checkpoint_catalog,
        catalog_configured,
        catalog_runtime
            .as_ref()
            .and_then(crate::cli::init::CatalogRuntime::customer_catalog),
        Arc::clone(&fs),
        shutdown.clone(),
    )
    .await;

    // Keep the metadata block cache warm so the first wave of reads (and the
    // reads right after every compaction, which replaces meta SSTs with cold
    // ones) doesn't serialize on object-store GETs of filters/indexes. Read-only
    // opens get no block cache (see `open_database`), so `subscribe_status`
    // returns `None` and warming is skipped there.
    if settings.cache.warm_metadata != crate::config::WarmMetadata::Off
        && let Some(status) = fs.db.subscribe_status()
    {
        let fs = Arc::clone(&fs);
        let warm_data = settings.cache.warm_metadata == crate::config::WarmMetadata::Full;
        let shutdown = shutdown.clone();
        let warm = async move {
            fs.db.warm_metadata_watch(warm_data, status, shutdown).await;
        };
        match &maintenance_runtime {
            Some(handle) => {
                handle.spawn(warm);
            }
            None => {
                tokio::spawn(warm);
            }
        }
    }

    let gc_handle = if !db_mode.is_read_only() {
        let tuning = crate::fs::gc::GcTuning::from(settings.gc.unwrap_or_default());
        let mut gc = GarbageCollector::new(
            Arc::clone(&fs.db),
            fs.tombstone_store.clone(),
            fs.extent_store.clone(),
            Arc::clone(&fs.stats),
            gc_admin,
            tuning,
        );
        if settings.storage.segment_pool_path.is_some() {
            gc = gc.without_segment_reclamation();
        }
        let gc = Arc::new(gc);
        Some(gc.start(shutdown.clone(), maintenance_runtime.clone()))
    } else {
        None
    };
    let stats_handle = start_stats_reporting(Arc::clone(&fs), shutdown.clone());
    let flush_handle = if !db_mode.is_read_only() {
        let flush_interval_secs = settings
            .lsm
            .map(|c| c.flush_interval_secs())
            .unwrap_or(crate::config::LsmConfig::DEFAULT_FLUSH_INTERVAL_SECS);
        Some(start_periodic_flush(
            Arc::clone(&fs),
            flush_interval_secs,
            shutdown.clone(),
        ))
    } else {
        None
    };

    #[cfg(feature = "webui")]
    let webui_handles = if let Some(ref webui_config) = settings.servers.webui {
        let webui_rpc_service = crate::rpc::server::AdminRpcServer::new_with_catalog(
            checkpoint_manager_for_webui,
            checkpoint_catalog_for_webui,
            catalog_configured,
            catalog_runtime
                .as_ref()
                .and_then(crate::cli::init::CatalogRuntime::customer_catalog),
            Arc::clone(&fs),
            shutdown.clone(),
        );
        let webui_lock_manager = Arc::new(crate::ninep::lock_manager::FileLockManager::new());
        crate::webui::start(
            webui_config,
            Arc::clone(&fs),
            webui_lock_manager,
            webui_rpc_service,
            shutdown.clone(),
        )
    } else {
        Vec::new()
    };

    let mut server_handles = Vec::new();
    server_handles.extend(nfs_handles);
    server_handles.extend(ninep_handles);
    server_handles.extend(nbd_handles);
    server_handles.extend(rpc_handles);
    #[cfg(feature = "webui")]
    server_handles.extend(webui_handles);

    if server_handles.is_empty() {
        shutdown.cancel();
        close_unserved_filesystem(
            &fs,
            db_mode,
            catalog_runtime.as_ref(),
            branch_writer.take(),
            None,
        )
        .await?;
        return Err(anyhow::anyhow!(
            "No servers configured. At least one server (NFS, 9P, NBD, or RPC) must be enabled."
        ));
    }

    let mut branch_lease_failed = false;
    let deposed = tokio::select! {
        biased;
        _ = leadership_deposed.cancelled() => {
            tracing::error!(
                "HA: this serving runtime was fenced or superseded; stopping without flushing \
                 the stale database"
            );
            true
        }
        _ = branch_lease_lost.cancelled(), if branch_writer.is_some() => {
            branch_lease_failed = true;
            tracing::error!(
                "configured branch writer authority was lost; initiating graceful close"
            );
            false
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT, initiating graceful shutdown...");
            false
        }
        _ = sigterm.recv() => {
            info!("Received SIGTERM, initiating graceful shutdown...");
            false
        }
    };

    info!("Cancelling all servers and background tasks...");
    shutdown.cancel();

    // Retain the join future so leadership loss cannot detach serving tasks.
    let mut serving_drain = Box::pin(futures::future::join_all(server_handles));

    let deposed_while_draining_servers = if deposed {
        true
    } else {
        info!("Waiting for servers to exit...");
        tokio::select! {
            biased;
            _ = leadership_deposed.cancelled() => true,
            _ = &mut serving_drain => false,
        }
    };
    if deposed_while_draining_servers {
        // A deposed database is not flushed. Serving transports get one bounded
        // interval to emit queued CLEAN responses.
        if tokio::time::timeout(
            crate::replication::RESPONSE_DRAIN_TIMEOUT,
            &mut serving_drain,
        )
        .await
        .is_err()
        {
            tracing::warn!("serving response drain timed out after leadership loss");
        }
        return Err(leadership_lost_error());
    }

    let drain = async move {
        info!("Waiting for background tasks to exit...");
        if let Some(gc_handles) = gc_handle {
            for handle in gc_handles {
                if tokio::time::timeout(std::time::Duration::from_secs(15), handle)
                    .await
                    .is_err()
                {
                    info!("a GC task is still mid-pass after 15s; proceeding to the final flush");
                }
            }
        }
        let _ = stats_handle.await;
        if let Some(flush_handle) = flush_handle {
            let _ = flush_handle.await;
        }
        if let Some(handle) = telemetry_handle {
            let _ = handle.await;
        }
        if let Some(handle) = digest_handle {
            let _ = handle.await;
        }
        for handle in prometheus_handles {
            let _ = handle.await;
        }
    };
    tokio::select! {
        biased;
        _ = leadership_deposed.cancelled() => {
            return Err(leadership_lost_error());
        }
        _ = drain => {}
    }

    // Flush remains lease-gated while background tasks drain.
    if leadership_deposed.is_cancelled() {
        return Err(leadership_lost_error());
    }
    let branch_grant = match branch_writer.take() {
        Some(writer) => Some(writer.stop().await),
        None => None,
    };
    info!("Performing final flush and closing database...");
    if db_mode.is_read_only() {
        if let Err(e) = fs.db.close().await {
            tracing::error!("Database close failed: {:?}", e);
            if let Err(catalog_error) = close_catalog_runtime(catalog_runtime.as_ref()).await {
                tracing::warn!("authoritative catalog close also failed: {catalog_error:#}");
            }
            return Err(e);
        }
    } else {
        let close_result = tokio::select! {
            biased;
            _ = leadership_deposed.cancelled() => {
                return Err(leadership_lost_error());
            }
            result = fs.flush_coordinator.close() => result,
        };
        if let Err(e) = close_result {
            // `db.close()` may flush metadata, so it is unsafe after seal failure.
            tracing::error!(
                "Final flush+close failed ({e:?}); exiting without a separate database close"
            );
            std::process::exit(1);
        }
    }

    if leadership_deposed.is_cancelled() {
        return Err(leadership_lost_error());
    }

    let publication_result = match (catalog_runtime.as_ref(), branch_grant.as_ref()) {
        (Some(runtime), Some(grant)) => runtime.publish_writer_head(grant).await,
        _ => Ok(()),
    };
    let catalog_close_result = close_catalog_runtime(catalog_runtime.as_ref()).await;

    // Retain authority monitors until both data and catalog databases close.
    if let Some(authority) = authority {
        authority.finish_after_close().await;
    }
    if leadership_deposed.is_cancelled() {
        // The catalog is already closed above.
        return Err(leadership_lost_error());
    }
    if let Err(publication_error) = publication_result {
        if let Err(catalog_error) = catalog_close_result {
            tracing::warn!(
                "authoritative catalog close also failed after writer-head publication error: {catalog_error:#}"
            );
        }
        return Err(publication_error);
    }
    catalog_close_result?;

    if branch_lease_failed {
        anyhow::bail!("configured branch writer lease renewal failed while serving");
    }

    info!("Shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use zerofs::catalog::{
        DurableRoot, LeaseAccessMode, LeaseGrant, LeaseRecord, LeaseSubjectKind,
    };

    fn test_branch_writer_grant(expires_after: chrono::Duration) -> LeaseGrant {
        let now = chrono::Utc::now();
        LeaseGrant {
            lease: LeaseRecord {
                id: Uuid::new_v4(),
                revision: 2,
                subject_kind: LeaseSubjectKind::Branch,
                subject_id: Uuid::new_v4(),
                root: DurableRoot {
                    identity: "branch".to_string(),
                    manifest_id: "root".to_string(),
                },
                access_mode: LeaseAccessMode::Write,
                token_hash: "test".to_string(),
                issued_at: now,
                updated_at: now,
                expires_at: now + expires_after,
            },
            renewal_token: Uuid::new_v4(),
        }
    }

    #[tokio::test]
    async fn initial_branch_writer_confirmation_times_out_then_recovers_applied_revision() {
        let initial = test_branch_writer_grant(chrono::Duration::milliseconds(250));
        let applied = Arc::new(tokio::sync::Mutex::new(initial.clone()));
        let attempt_applied = Arc::clone(&applied);
        let attempt = async move {
            attempt_applied.lock().await.lease.revision = 9;
            std::future::pending::<Result<LeaseGrant>>().await
        };

        tokio::time::timeout(
            Duration::from_millis(500),
            confirm_branch_writer_before_serving(&initial, Duration::from_millis(100), attempt),
        )
        .await
        .expect("initial renewal confirmation must be bounded")
        .expect_err("stalled initial renewal must not be treated as confirmed");

        let recovered = recover_branch_writer_bounded(Duration::from_millis(100), async {
            Ok(applied.lock().await.clone())
        })
        .await
        .expect("exact recovery should retain an applied renewal revision");
        assert_eq!(recovered.lease.id, initial.lease.id);
        assert_eq!(recovered.renewal_token, initial.renewal_token);
        assert_eq!(recovered.lease.revision, 9);
    }

    #[tokio::test]
    async fn initial_branch_writer_recovery_is_bounded() {
        tokio::time::timeout(
            Duration::from_millis(200),
            recover_branch_writer_bounded(
                Duration::from_millis(50),
                std::future::pending::<Result<LeaseGrant>>(),
            ),
        )
        .await
        .expect("outer test timeout must not fire")
        .expect_err("stalled exact recovery must time out");
    }

    #[tokio::test]
    async fn branch_writer_supervisor_renews_and_returns_latest_grant() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let initial = test_branch_writer_grant(chrono::Duration::seconds(1));
        let renewals = Arc::new(AtomicU64::new(0));
        let renewer: BranchWriterRenewer = Arc::new(move |mut grant, duration| {
            let renewals = Arc::clone(&renewals);
            Box::pin(async move {
                renewals.fetch_add(1, Ordering::SeqCst);
                grant.lease.revision += 1;
                grant.lease.updated_at = chrono::Utc::now();
                grant.lease.expires_at = grant.lease.updated_at + duration;
                Ok(grant)
            })
        });
        let supervisor = BranchWriterLeaseSupervisor::spawn_after_confirmation(initial, 1, renewer);
        let lost = supervisor.loss_token();
        tokio::time::sleep(Duration::from_millis(450)).await;
        let latest = supervisor.stop().await;
        assert!(latest.lease.revision >= 3);
        assert!(latest.lease.expires_at > chrono::Utc::now());
        assert!(!lost.is_cancelled());
    }

    #[tokio::test]
    async fn branch_writer_supervisor_retains_reconciled_grant_on_renewal_failure() {
        let initial = test_branch_writer_grant(chrono::Duration::seconds(1));
        let renewer: BranchWriterRenewer = Arc::new(move |mut grant, _| {
            Box::pin(async move {
                grant.lease.revision = 9;
                Err(BranchWriterRenewalFailure {
                    latest: grant,
                    error: anyhow::anyhow!("lost renewal response"),
                })
            })
        });
        let supervisor = BranchWriterLeaseSupervisor::spawn_after_confirmation(initial, 0, renewer);
        let lost = supervisor.loss_token();
        tokio::time::timeout(Duration::from_millis(500), lost.cancelled())
            .await
            .expect("renewal failure must revoke serving");
        assert_eq!(supervisor.stop().await.lease.revision, 9);
    }

    #[tokio::test]
    async fn branch_writer_supervisor_bounds_stalled_renewal_and_shutdown() {
        let initial = test_branch_writer_grant(chrono::Duration::milliseconds(250));
        let renewer: BranchWriterRenewer = Arc::new(move |_, _| {
            Box::pin(std::future::pending::<
                std::result::Result<LeaseGrant, BranchWriterRenewalFailure>,
            >())
        });
        let mut supervisor =
            BranchWriterLeaseSupervisor::spawn_after_confirmation(initial, 0, renewer);
        supervisor.reconciler = Some(Arc::new(move |mut grant, _| {
            Box::pin(async move {
                grant.lease.revision = 9;
                Ok(grant)
            })
        }));
        let lost = supervisor.loss_token();
        tokio::time::timeout(Duration::from_millis(500), lost.cancelled())
            .await
            .expect("stalled renewal must revoke serving before confirmed expiry");
        let latest = tokio::time::timeout(Duration::from_millis(500), supervisor.stop())
            .await
            .expect("stalled renewal must not hang shutdown");
        assert_eq!(latest.lease.revision, 9);
    }

    #[tokio::test]
    async fn branch_writer_supervisor_reconciles_clean_stop_cancellation() {
        let initial = test_branch_writer_grant(chrono::Duration::seconds(1));
        let renewer: BranchWriterRenewer = Arc::new(move |_, _| {
            Box::pin(std::future::pending::<
                std::result::Result<LeaseGrant, BranchWriterRenewalFailure>,
            >())
        });
        let mut supervisor =
            BranchWriterLeaseSupervisor::spawn_after_confirmation(initial, 0, renewer);
        supervisor.reconciler = Some(Arc::new(move |mut grant, _| {
            Box::pin(async move {
                grant.lease.revision = 7;
                Ok(grant)
            })
        }));
        assert_eq!(supervisor.stop().await.lease.revision, 7);
    }

    #[tokio::test]
    async fn branch_writer_supervisor_revokes_serving_when_worker_panics() {
        let initial = test_branch_writer_grant(chrono::Duration::seconds(1));
        let renewer: BranchWriterRenewer = Arc::new(move |_, _| {
            Box::pin(async move {
                panic!("injected renewal panic");
            })
        });
        let supervisor = BranchWriterLeaseSupervisor::spawn_after_confirmation(initial, 0, renewer);
        let lost = supervisor.loss_token();
        tokio::time::timeout(Duration::from_millis(500), lost.cancelled())
            .await
            .expect("renewal worker panic must revoke serving");
        tokio::time::timeout(Duration::from_millis(500), supervisor.stop())
            .await
            .expect("panicked renewal worker must not hang shutdown");
    }

    #[test]
    fn barrier_controlled_flush_thresholds_are_valid() {
        let settings = slatedb::config::Settings {
            l0_sst_size_bytes: BARRIER_CONTROLLED_L0_SST_SIZE_BYTES,
            max_unflushed_bytes: BARRIER_CONTROLLED_MAX_UNFLUSHED_BYTES,
            ..Default::default()
        };

        settings
            .validate()
            .expect("barrier-controlled flush thresholds must satisfy SlateDB validation");
    }

    #[test]
    fn maintenance_runtime_is_shared_across_open_attempts() {
        let first = shared_maintenance_runtime();
        let second = shared_maintenance_runtime();
        assert!(std::ptr::eq(first, second));

        let (tx, rx) = std::sync::mpsc::channel();
        first.spawn(async move {
            tx.send(()).unwrap();
        });
        rx.recv_timeout(std::time::Duration::from_secs(5))
            .expect("shared maintenance runtime did not execute a task");
    }

    #[test]
    fn split_disk_budget_favors_segments() {
        let gib = 1024 * 1024 * 1024;
        // 10% to metadata, the rest to segments.
        assert_eq!(split_disk_budget(100 * gib), (90 * gib, 10 * gib));
        // Metadata capped at 16 GiB on a huge budget; segments get everything else.
        assert_eq!(split_disk_budget(4096 * gib), (4080 * gib, 16 * gib));
        // Tiny budgets still floor each side at 1 GiB.
        assert_eq!(split_disk_budget(gib / 2), (gib, gib));
    }

    #[test]
    fn parts_engine_knobs_scale_with_device() {
        let mib = 1024 * 1024;
        let gib = 1024 * mib;
        let large = parts_engine_knobs(583 * gib);
        assert_eq!(large.flushers, 4);
        assert_eq!(large.clean_block_threshold, 8);
        assert_eq!(large.submit_queue_bytes, gib);
        assert_eq!(large.buffer_pool_bytes, 256 * mib);

        let floor = parts_engine_knobs(gib);
        assert_eq!(floor.flushers, 2);
        assert_eq!(floor.clean_block_threshold, 2);
        assert_eq!(floor.submit_queue_bytes, 256 * mib);
        assert_eq!(floor.buffer_pool_bytes, 128 * mib);

        // Degenerate device (below one block): everything at its floor.
        let tiny = parts_engine_knobs(mib);
        assert_eq!(tiny.flushers, 1);
        assert_eq!(tiny.clean_block_threshold, 1);
        assert_eq!(tiny.submit_queue_bytes, 16 * mib);
        assert_eq!(tiny.buffer_pool_bytes, 64 * mib);
    }

    #[test]
    fn split_memory_budget_favors_segments() {
        let mib = 1024 * 1024;
        let gib = 1024 * mib;
        // 25% to metadata blocks, the rest to segment parts.
        assert_eq!(split_memory_budget(gib), (768 * mib, 256 * mib));
        // Metadata capped at 2 GiB on a huge budget; parts get everything else.
        assert_eq!(split_memory_budget(40 * gib), (38 * gib, 2 * gib));
        // Tiny budgets floor each side at 32 MiB.
        assert_eq!(split_memory_budget(16 * mib), (32 * mib, 32 * mib));
    }

    // foyer builds the same `I/O error => coding error` wrapping around the os
    // error regardless of the build site, so exercise its own From<io::Error>.
    fn foyer_os_error(raw: i32) -> foyer::Error {
        std::io::Error::from_raw_os_error(raw).into()
    }

    #[test]
    fn emfile_is_detected_and_hinted() {
        let err = foyer_os_error(libc::EMFILE);
        assert!(is_fd_exhaustion(&err));
        let msg = foyer_build_error("foyer hybrid build failed", err).to_string();
        assert!(
            msg.contains("foyer hybrid build failed"),
            "lost context: {msg}"
        );
        assert!(msg.contains("ulimit -n"), "missing fd hint: {msg}");
    }

    #[test]
    fn enfile_is_detected() {
        assert!(is_fd_exhaustion(&foyer_os_error(libc::ENFILE)));
    }

    #[test]
    fn other_io_errors_get_no_hint() {
        let err = foyer_os_error(libc::ENOSPC);
        assert!(!is_fd_exhaustion(&err));
        let msg = foyer_build_error("foyer device build failed", err).to_string();
        assert!(!msg.contains("ulimit"), "spurious fd hint: {msg}");
    }

    mod warm_metadata {
        use super::*;
        use crate::fault_store::{FaultControls, FaultStore};
        use crate::fs::key_codec::KeyCodec;
        use bytes::Bytes;
        use object_store::ObjectStore;
        use slatedb::config::WriteOptions;
        use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
        use slatedb::{SstBlockSize, WriteBatch};
        use std::sync::Arc;

        const INODES: u64 = 8_000;
        // Sample keys spread across the keyspace so the cold reads touch many
        // distinct SST data blocks, not just one.
        const SAMPLE_STRIDE: u64 = 400;

        async fn hybrid(root: &std::path::Path) -> Arc<FoyerHybridCache> {
            build_block_hybrid(
                root,
                64 * 1024 * 1024,
                512 * 1024 * 1024,
                &tokio::runtime::Handle::current(),
            )
            .await
            .expect("foyer hybrid")
        }

        // Open a writer over `store` with the same segment/filter/block config the
        // server uses, so writes route into the `meta` segment exactly as in prod.
        async fn open(store: Arc<dyn ObjectStore>, cache: Arc<FoyerHybridCache>) -> slatedb::Db {
            // Small L0s so the 8k rows freeze into several SSTs, exercising the
            // warm fan-out over more than one SST.
            // No compactor: the 4 setup L0s meet the default compaction
            // threshold, and a background compaction racing into a measured
            // window charges its GETs there (and un-warms the cache by
            // swapping the manifest to fresh SSTs).
            let settings = slatedb::config::Settings {
                l0_sst_size_bytes: 64 * 1024,
                compactor_options: None,
                ..Default::default()
            };
            slatedb::DbBuilder::new(slatedb::object_store::path::Path::from("db"), store)
                .with_settings(settings)
                .with_db_cache(cache)
                .with_sst_block_size(SstBlockSize::Block32Kib)
                .with_filter_policies(crate::fs::filter_policy::filter_policies())
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
                .build()
                .await
                .expect("open slatedb")
        }

        // Object-store GETs charged while reading the sample keys cold.
        async fn read_sample_gets(
            db: &crate::db::Db,
            codec: &KeyCodec,
            ctl: &FaultControls,
        ) -> usize {
            let before = ctl.get_count();
            let mut id = 0;
            while id < INODES {
                let v = db
                    .get_bytes(&codec.inode_key(id))
                    .await
                    .expect("get")
                    .expect("inode present");
                assert_eq!(v.len(), 64);
                id += SAMPLE_STRIDE;
            }
            ctl.get_count() - before
        }

        /// A cold `Db` (fresh foyer cache, all metadata on the object store)
        /// pays object-store GETs for SST filters/indexes/data on its first
        /// reads. `warm_metadata` pulls the `meta` segment into cache up front,
        /// so the same reads issue no object-store GETs. The bulk segment, which
        /// these keys don't touch, is irrelevant. Real foyer cache + real
        /// LocalFileSystem store; GETs counted by the FaultStore decorator.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn warm_eliminates_cold_metadata_gets() {
            let dir = tempfile::tempdir().unwrap();
            let store_root = dir.path().join("store");
            std::fs::create_dir_all(&store_root).unwrap();
            let local = Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&store_root).unwrap(),
            );
            let (store, ctl) = FaultStore::new(local);
            let store: Arc<dyn ObjectStore> = store;
            let codec = KeyCodec::new();

            // Setup: write the metadata and persist it to SSTs, in several flushes
            // so the meta segment ends up with more than one SST.
            {
                let raw = open(store.clone(), hybrid(&dir.path().join("c_setup")).await).await;
                for extent in 0..4u64 {
                    let mut batch = WriteBatch::new();
                    for i in 0..(INODES / 4) {
                        let id = extent * (INODES / 4) + i;
                        batch.put_bytes(codec.inode_key(id), Bytes::from(vec![id as u8; 64]));
                    }
                    raw.write_with_options(
                        batch,
                        &WriteOptions {
                            await_durable: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                    raw.flush().await.unwrap();
                }
                raw.close().await.unwrap();
            }

            // Cold read, no warm: reopen with a fresh cache and read the samples.
            let cold_gets = {
                let raw = open(store.clone(), hybrid(&dir.path().join("c_cold")).await).await;
                let db = crate::db::Db::new(Arc::new(raw), None);
                let gets = read_sample_gets(&db, &codec, &ctl).await;
                db.close().await.unwrap();
                gets
            };

            // Cold read, warmed: reopen with a fresh cache, warm the meta segment,
            // then read the same samples.
            let (warm_gets, warm_second, warmed) = {
                let raw = open(store.clone(), hybrid(&dir.path().join("c_warm")).await).await;
                let db = crate::db::Db::new(Arc::new(raw), None);
                let warmed = db.warm_metadata(true).await;
                let gets = read_sample_gets(&db, &codec, &ctl).await;
                // A second pass over the same keys: warm + first-touch should have
                // left the whole metadata working set in cache.
                let second = read_sample_gets(&db, &codec, &ctl).await;
                db.close().await.unwrap();
                (gets, second, warmed)
            };

            assert!(
                warmed.ssts >= 2,
                "expected the meta segment to span several SSTs, got {}",
                warmed.ssts
            );
            assert_eq!(warmed.failed, 0, "warm should not fail any SST");
            assert!(
                cold_gets > 0,
                "cold reads must hit the object store, got {cold_gets}"
            );
            // Warming collapses the cold read cost by a wide margin. It is not
            // exactly zero: `warm_sst` reuses the manifest's SST handles and so
            // intentionally skips the per-SST footer `open_sst` GET the read path
            // still pays once on first access (~2 per SST), plus the foyer hybrid
            // cache's async disk tier can require an occasional re-fetch. So both
            // warmed passes must stay far below cold, not necessarily at zero.
            assert!(
                warm_gets * 2 <= cold_gets,
                "warming should cut cold GETs by a wide margin: cold={cold_gets} warm={warm_gets}"
            );
            assert!(
                warm_second * 2 <= cold_gets,
                "reads after warming must stay far below cold: cold={cold_gets} second={warm_second}"
            );
        }
    }
}
