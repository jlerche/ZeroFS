use super::{
    BranchLifecycle, BranchRecord, BranchState, Catalog, CatalogError, CatalogMutation,
    CatalogProjection, CheckpointRecord, CustomerCatalogListRequest, CustomerMetadata, GcRunPhase,
    ImmutableCheckpoint, JsonCatalogProjection, PostgresCatalogProjection,
    RootCaptureLifecycleError, ServerWriterMountRequest, SlateDbCatalog, SlateDbRootStore,
    catalog_timestamp,
};
use crate::fault_store::{FaultControls, FaultStore};
use crate::fs::key_codec::KeyCodec;
use crate::segment::{FrameLoc, Segid};
use bytes::Bytes;
use futures::{StreamExt, stream};
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::Value;
use slatedb::admin::AdminBuilder;
use slatedb::config::{CheckpointOptions, CheckpointScope, WriteOptions};
use slatedb::object_store::path::Path;
use slatedb::{Db, WriteBatch};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

const STRESS_CONFIRMATION: &str = "run-cow-minio-postgres-stress-with-retained-prefix";
const BRANCH_BENCH_CONFIRMATION: &str = "run-cow-branch-create-benchmark-with-retained-prefix";

#[derive(Clone)]
struct BranchSpec {
    operation_id: Uuid,
    deletion_operation_id: Uuid,
    branch_id: Uuid,
    name: String,
}

fn bounded_env(name: &str, default: usize, minimum: usize, maximum: usize) -> usize {
    let value = std::env::var(name)
        .ok()
        .map(|raw| {
            raw.parse::<usize>()
                .unwrap_or_else(|error| panic!("{name} must be an integer: {error}"))
        })
        .unwrap_or(default);
    assert!(
        (minimum..=maximum).contains(&value),
        "{name} must be within {minimum}..={maximum}"
    );
    value
}

#[derive(Clone, Copy, Debug)]
enum BranchBenchProjection {
    Authority,
    Postgres,
    Json,
}

impl BranchBenchProjection {
    fn from_env() -> Self {
        match std::env::var("ZEROFS_BRANCH_BENCH_PROJECTION")
            .unwrap_or_else(|_| "authority".to_string())
            .as_str()
        {
            "authority" => Self::Authority,
            "postgres" => Self::Postgres,
            "json" => Self::Json,
            value => panic!(
                "ZEROFS_BRANCH_BENCH_PROJECTION must be authority, postgres, or json; got {value}"
            ),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Authority => "authority",
            Self::Postgres => "postgres",
            Self::Json => "json",
        }
    }
}

#[derive(Clone, Copy)]
struct ObjectCounts {
    gets: usize,
    puts: usize,
    lists: usize,
}

impl ObjectCounts {
    fn read(counters: &FaultControls) -> Self {
        Self {
            gets: counters.get_count(),
            puts: counters.put_count(),
            lists: counters.list_count(),
        }
    }

    fn delta(self, before: Self) -> Self {
        Self {
            gets: self.gets - before.gets,
            puts: self.puts - before.puts,
            lists: self.lists - before.lists,
        }
    }
}

fn percentile(sorted_micros: &[u64], percentile: usize) -> u64 {
    assert!(!sorted_micros.is_empty());
    assert!((1..=100).contains(&percentile));
    let rank = (percentile * sorted_micros.len()).div_ceil(100);
    sorted_micros[rank.saturating_sub(1)]
}

fn wal_off_settings() -> slatedb::config::Settings {
    slatedb::config::Settings {
        wal_enabled: false,
        l0_sst_size_bytes: usize::MAX - 1,
        max_unflushed_bytes: usize::MAX,
        ..Default::default()
    }
}

fn stress_url(raw: &str, confirmation: Option<&str>) -> Result<url::Url, String> {
    qualified_s3_url(raw, confirmation, STRESS_CONFIRMATION, "CoW stress")
}

fn branch_bench_url(raw: &str, confirmation: Option<&str>) -> Result<url::Url, String> {
    qualified_s3_url(
        raw,
        confirmation,
        BRANCH_BENCH_CONFIRMATION,
        "branch benchmark",
    )
}

fn qualified_s3_url(
    raw: &str,
    confirmation: Option<&str>,
    expected_confirmation: &str,
    label: &str,
) -> Result<url::Url, String> {
    let url: url::Url = raw
        .parse()
        .map_err(|error| format!("{label} URL is invalid: {error}"))?;
    if !matches!(url.scheme(), "s3" | "s3a") {
        return Err(format!(
            "{label} requires an S3-compatible object store such as MinIO"
        ));
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("stress credentials and options must come from the environment".to_string());
    }
    if confirmation != Some(expected_confirmation) {
        return Err(format!(
            "{label} confirmation must equal {expected_confirmation}"
        ));
    }
    Ok(url)
}

async fn assert_projection_parity(
    volume_id: Uuid,
    json: &JsonCatalogProjection,
    postgres: &PostgresCatalogProjection,
) {
    let mut after = None;
    loop {
        let request = CustomerCatalogListRequest {
            kind: None,
            parent_id: None,
            state: None,
            after,
            limit: 64,
        };
        let json_page = json.list(volume_id, request.clone()).await.unwrap();
        let postgres_page = postgres.list(volume_id, request).await.unwrap();
        assert_eq!(json_page, postgres_page);
        after = json_page.next_after;
        if after.is_none() {
            break;
        }
    }
}

async fn run_report(lifecycle: &BranchLifecycle, run_id: Uuid) -> super::GcRunRecord {
    let gc = lifecycle.root_captures();
    gc.begin(run_id).await.unwrap();
    gc.mark(run_id).await.unwrap();
    let reported = gc.report(run_id).await.unwrap();
    assert_eq!(reported.phase, GcRunPhase::Reported);
    assert_eq!(gc.begin(run_id).await.unwrap(), reported);
    assert_eq!(gc.mark(run_id).await.unwrap(), reported);
    assert_eq!(gc.report(run_id).await.unwrap(), reported);
    reported
}

async fn create_benchmark_batch(
    lifecycle: Arc<BranchLifecycle>,
    specs: Vec<BranchSpec>,
    source_branch_id: Uuid,
    concurrency: usize,
    volume_id: Uuid,
    projection: Option<Arc<dyn CatalogProjection>>,
) -> Vec<u64> {
    stream::iter(specs)
        .map(|spec| {
            let lifecycle = Arc::clone(&lifecycle);
            let projection = projection.clone();
            async move {
                let started = Instant::now();
                let branch = lifecycle
                    .create_from_checkpoint_name_by_identity(
                        spec.operation_id,
                        spec.branch_id,
                        spec.name,
                        source_branch_id,
                        "branch-bench-source".to_string(),
                    )
                    .await
                    .unwrap();
                assert_eq!(branch.state, BranchState::Ready);
                if let Some(projection) = projection {
                    lifecycle
                        .reconcile_projection(volume_id, projection.as_ref())
                        .await
                        .unwrap();
                }
                u64::try_from(started.elapsed().as_micros()).unwrap()
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await
}

/// Focused release-mode throughput benchmark for the production checkpoint
/// branch-creation protocol. Setup, warm-up, validation, and cleanup are
/// excluded from the timed interval. Every successful timed operation executes
/// reserve -> physical COW clone -> root authentication -> catalog publication.
/// `postgres` and `json` modes additionally include the same synchronous,
/// best-effort projection reconciliation attempted by the server RPC boundary.
///
/// Required environment:
/// - `ZEROFS_BRANCH_BENCH_URL`: retained S3-compatible prefix
/// - `ZEROFS_BRANCH_BENCH_CONFIRM=run-cow-branch-create-benchmark-with-retained-prefix`
/// - PostgreSQL mode also requires `ZEROFS_BRANCH_BENCH_POSTGRES_URL`
///
/// Knobs: `_BRANCHES` (default 256), `_REFERENCES` (default 1), `_SEGMENTS`
/// (default 1), `_CONCURRENCY` (default 32), `_WARMUP` (default 8), and
/// `_PROJECTION=authority|postgres|json` (default authority).
#[tokio::test]
#[ignore = "requires explicitly acknowledged MinIO/S3 and optional disposable PostgreSQL"]
async fn cow_branch_create_throughput() {
    let timeout_minutes = bounded_env("ZEROFS_BRANCH_BENCH_TIMEOUT_MINUTES", 30, 1, 120);
    tokio::time::timeout(
        std::time::Duration::from_secs(timeout_minutes as u64 * 60),
        run_cow_branch_create_throughput(),
    )
    .await
    .unwrap_or_else(|_| panic!("branch creation benchmark exceeded {timeout_minutes} minutes"));
}

async fn run_cow_branch_create_throughput() {
    let raw_url = std::env::var("ZEROFS_BRANCH_BENCH_URL")
        .expect("ZEROFS_BRANCH_BENCH_URL must name an S3-compatible prefix");
    let url = branch_bench_url(
        &raw_url,
        std::env::var("ZEROFS_BRANCH_BENCH_CONFIRM").ok().as_deref(),
    )
    .unwrap();
    let branches = bounded_env("ZEROFS_BRANCH_BENCH_BRANCHES", 256, 1, 4_095);
    let references = bounded_env("ZEROFS_BRANCH_BENCH_REFERENCES", 1, 1, 1_000_000);
    let segments = bounded_env("ZEROFS_BRANCH_BENCH_SEGMENTS", 1, 1, references);
    let concurrency = bounded_env("ZEROFS_BRANCH_BENCH_CONCURRENCY", 32, 1, 64);
    let warmup = bounded_env("ZEROFS_BRANCH_BENCH_WARMUP", 8, 0, 64);
    assert!(
        branches + warmup <= 4_095,
        "timed plus warm-up branches must leave capacity for the source branch"
    );
    let projection_mode = BranchBenchProjection::from_env();
    let postgres_tls = std::env::var("ZEROFS_BRANCH_BENCH_POSTGRES_TLS")
        .map(|value| value != "false")
        .unwrap_or(true);

    let (parsed, configured_prefix) = object_store::parse_url_opts(&url, std::env::vars()).unwrap();
    let qualification_id = Uuid::new_v4();
    let retained_prefix = configured_prefix
        .join("zerofs-branch-create-bench")
        .join(qualification_id.to_string());
    let retained_prefix_text = retained_prefix.to_string();
    let scoped = object_store::prefix::PrefixStore::new(parsed, retained_prefix);
    let (counted_store, counters) = FaultStore::new(Arc::new(scoped));
    let store: Arc<dyn ObjectStore> = counted_store;
    let source_path = Path::from("source");
    let catalog_path = Path::from("catalog");
    let branch_root = Path::from("branches");
    let segment_pool = Path::from("segment-pool");
    let volume_id = Uuid::new_v4();
    let setup_started = Instant::now();

    println!(
        "{}",
        serde_json::json!({
            "event": "branch_create_benchmark_started",
            "qualification_id": qualification_id,
            "retained_object_prefix": retained_prefix_text,
            "automatic_cleanup": false,
            "branches": branches,
            "references": references,
            "segments": segments,
            "concurrency": concurrency,
            "warmup": warmup,
            "projection": projection_mode.as_str(),
            "postgres_tls": postgres_tls,
        })
    );

    let source_db = Db::builder(source_path.clone(), Arc::clone(&store))
        .with_settings(wal_off_settings())
        .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
        .build()
        .await
        .unwrap();
    let key_codec = KeyCodec::new();
    let mut source_batch = WriteBatch::new();
    for index in 0..references {
        source_batch.put(
            key_codec.extent_key(index as u64 + 1, 0),
            FrameLoc {
                segid: Segid::new(1, (index % segments) as u64),
                frame_index: 0,
                byte_offset: 0,
                byte_len: 1,
            }
            .encode(),
        );
    }
    source_db
        .write_with_options(
            source_batch,
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    source_db.flush().await.unwrap();
    let source_checkpoint = source_db
        .create_checkpoint(
            CheckpointScope::All,
            &CheckpointOptions {
                lifetime: None,
                source: None,
                name: Some("branch-bench-source".to_string()),
            },
        )
        .await
        .unwrap();
    source_db.close().await.unwrap();
    let physical = AdminBuilder::new(source_path.clone(), Arc::clone(&store))
        .build()
        .list_checkpoints(Some("branch-bench-source"))
        .await
        .unwrap()
        .into_iter()
        .find(|checkpoint| checkpoint.id == source_checkpoint.id)
        .unwrap();
    let source = ImmutableCheckpoint {
        database_path: source_path,
        checkpoint_id: source_checkpoint.id,
        manifest_id: source_checkpoint.manifest_id,
    };
    let now = catalog_timestamp(physical.create_time);
    let source_branch_id = Uuid::new_v4();
    let catalog = Arc::new(
        SlateDbCatalog::open(catalog_path, Arc::clone(&store))
            .await
            .unwrap(),
    );
    catalog
        .apply(CatalogMutation::CreateBranch(BranchRecord {
            id: source_branch_id,
            revision: 1,
            name: "branch-bench-main".to_string(),
            state: BranchState::Ready,
            root: Some(source.durable_root()),
            parent_id: None,
            origin_checkpoint_id: None,
            created_at: now,
            updated_at: now,
        }))
        .await
        .unwrap();
    catalog
        .apply(CatalogMutation::CreateCheckpoint(CheckpointRecord {
            id: source.checkpoint_id,
            revision: 1,
            branch_id: source_branch_id,
            name: "branch-bench-source".to_string(),
            root: source.durable_root(),
            created_at: now,
            updated_at: now,
        }))
        .await
        .unwrap();
    let lifecycle = Arc::new(BranchLifecycle::new(
        catalog.clone(),
        SlateDbRootStore::new(Arc::clone(&store), branch_root).with_segment_pool_root(segment_pool),
    ));

    let json_directory = matches!(projection_mode, BranchBenchProjection::Json)
        .then(|| tempfile::tempdir().unwrap());
    let projection: Option<Arc<dyn CatalogProjection>> = match projection_mode {
        BranchBenchProjection::Authority => None,
        BranchBenchProjection::Postgres => {
            let postgres_url = std::env::var("ZEROFS_BRANCH_BENCH_POSTGRES_URL")
                .expect("PostgreSQL projection mode requires ZEROFS_BRANCH_BENCH_POSTGRES_URL");
            let postgres = PostgresCatalogProjection::connect_with_tls(&postgres_url, postgres_tls)
                .await
                .unwrap();
            postgres.migrate().await.unwrap();
            Some(Arc::new(postgres))
        }
        BranchBenchProjection::Json => Some(Arc::new(JsonCatalogProjection::new(
            json_directory
                .as_ref()
                .unwrap()
                .path()
                .join("projection.json"),
        ))),
    };
    if let Some(projection) = &projection {
        lifecycle
            .reconcile_projection(volume_id, projection.as_ref())
            .await
            .unwrap();
    }

    let warmup_specs = (0..warmup)
        .map(|index| BranchSpec {
            operation_id: Uuid::new_v4(),
            deletion_operation_id: Uuid::new_v4(),
            branch_id: Uuid::new_v4(),
            name: format!("branch-bench-warmup-{index:04}"),
        })
        .collect();
    create_benchmark_batch(
        Arc::clone(&lifecycle),
        warmup_specs,
        source_branch_id,
        concurrency,
        volume_id,
        projection.clone(),
    )
    .await;

    let setup_ms = setup_started.elapsed().as_millis();
    let specs = (0..branches)
        .map(|index| BranchSpec {
            operation_id: Uuid::new_v4(),
            deletion_operation_id: Uuid::new_v4(),
            branch_id: Uuid::new_v4(),
            name: format!("branch-bench-child-{index:04}"),
        })
        .collect();
    let counts_before = ObjectCounts::read(counters.as_ref());
    let timed_started = Instant::now();
    let mut latency_micros = create_benchmark_batch(
        Arc::clone(&lifecycle),
        specs,
        source_branch_id,
        concurrency,
        volume_id,
        projection.clone(),
    )
    .await;
    let timed_elapsed = timed_started.elapsed();
    let counts = ObjectCounts::read(counters.as_ref()).delta(counts_before);
    latency_micros.sort_unstable();

    let snapshot = catalog.snapshot().await.unwrap();
    snapshot.validate().unwrap();
    assert_eq!(snapshot.branches.len(), 1 + warmup + branches);
    assert_eq!(
        snapshot
            .branches
            .values()
            .filter(|branch| branch.state == BranchState::Ready)
            .count(),
        1 + warmup + branches
    );
    if let Some(projection) = &projection {
        lifecycle
            .reconcile_projection(volume_id, projection.as_ref())
            .await
            .unwrap();
        let mut projected = 0;
        let mut after = None;
        loop {
            let page = projection
                .list(
                    volume_id,
                    CustomerCatalogListRequest {
                        kind: None,
                        parent_id: None,
                        state: None,
                        after,
                        limit: 256,
                    },
                )
                .await
                .unwrap();
            projected += page.records.len();
            after = page.next_after;
            if after.is_none() {
                break;
            }
        }
        assert_eq!(projected, 2 + warmup + branches);
    }

    let elapsed_seconds = timed_elapsed.as_secs_f64();
    println!(
        "{}",
        serde_json::json!({
            "event": "branch_create_benchmark_completed",
            "qualification_id": qualification_id,
            "retained_object_prefix": retained_prefix_text,
            "automatic_cleanup": false,
            "projection": projection_mode.as_str(),
            "branches": branches,
            "references": references,
            "segments": segments,
            "concurrency": concurrency,
            "warmup": warmup,
            "setup_ms": setup_ms,
            "timed_ms": timed_elapsed.as_millis(),
            "branches_per_second": branches as f64 / elapsed_seconds,
            "latency_micros": {
                "p50": percentile(&latency_micros, 50),
                "p95": percentile(&latency_micros, 95),
                "p99": percentile(&latency_micros, 99),
                "min": latency_micros[0],
                "max": latency_micros[latency_micros.len() - 1],
            },
            "object_store": {
                "gets": counts.gets,
                "puts": counts.puts,
                "lists": counts.lists,
                "requests_per_branch": (counts.gets + counts.puts + counts.lists) as f64
                    / branches as f64,
            },
        })
    );
    lifecycle.close().await.unwrap();
}

/// Expensive manual stress qualification for the production object-store,
/// lifecycle, projection, and terminal shadow-GC paths. It retains a unique
/// object prefix on success and failure so unexpected state can be inspected.
///
/// Defaults: 128 concurrent branches, 16,384 references over 4,096 reachable
/// segments, 512 unreachable inventory objects, and concurrency 32. Scale can
/// be adjusted with `ZEROFS_COW_STRESS_BRANCHES`, `_REFERENCES`, `_SEGMENTS`,
/// `_CANDIDATES`, and `_CONCURRENCY`.
///
/// Example against MinIO and plaintext local PostgreSQL:
/// `ZEROFS_COW_STRESS_URL=s3://bucket/prefix \
///  ZEROFS_COW_STRESS_CONFIRM=run-cow-minio-postgres-stress-with-retained-prefix \
///  ZEROFS_COW_STRESS_POSTGRES_URL=postgresql://user:pass@127.0.0.1/db \
///  ZEROFS_COW_STRESS_POSTGRES_TLS=false \
///  cargo test --release --lib cow_minio_postgres_lifecycle_stress \
///  -- --ignored --nocapture`
#[tokio::test]
#[ignore = "requires explicitly acknowledged MinIO/S3 and disposable PostgreSQL targets"]
async fn cow_minio_postgres_lifecycle_stress() {
    let timeout_minutes = bounded_env("ZEROFS_COW_STRESS_TIMEOUT_MINUTES", 30, 1, 120);
    tokio::time::timeout(
        std::time::Duration::from_secs(timeout_minutes as u64 * 60),
        run_cow_minio_postgres_lifecycle_stress(),
    )
    .await
    .unwrap_or_else(|_| panic!("CoW stress exceeded {timeout_minutes} minutes"));
}

async fn run_cow_minio_postgres_lifecycle_stress() {
    let raw_url = std::env::var("ZEROFS_COW_STRESS_URL")
        .expect("ZEROFS_COW_STRESS_URL must name an S3-compatible prefix");
    let url = stress_url(
        &raw_url,
        std::env::var("ZEROFS_COW_STRESS_CONFIRM").ok().as_deref(),
    )
    .unwrap();
    let postgres_url = std::env::var("ZEROFS_COW_STRESS_POSTGRES_URL")
        .expect("ZEROFS_COW_STRESS_POSTGRES_URL must name a disposable database");
    let postgres_tls = std::env::var("ZEROFS_COW_STRESS_POSTGRES_TLS")
        .map(|value| value != "false")
        .unwrap_or(true);
    let branches = bounded_env("ZEROFS_COW_STRESS_BRANCHES", 128, 2, 255);
    let references = bounded_env("ZEROFS_COW_STRESS_REFERENCES", 16_384, 1, 1_000_000);
    let segments = bounded_env("ZEROFS_COW_STRESS_SEGMENTS", 4_096, 1, references);
    let candidates = bounded_env("ZEROFS_COW_STRESS_CANDIDATES", 512, 1, 100_000);
    let concurrency = bounded_env("ZEROFS_COW_STRESS_CONCURRENCY", 32, 1, 64);

    let (parsed, configured_prefix) = object_store::parse_url_opts(&url, std::env::vars()).unwrap();
    let qualification_id = Uuid::new_v4();
    let retained_prefix = configured_prefix
        .join("zerofs-cow-stress")
        .join(qualification_id.to_string());
    let retained_prefix_text = retained_prefix.to_string();
    let scoped = object_store::prefix::PrefixStore::new(parsed, retained_prefix);
    let (store, counters) = FaultStore::new(Arc::new(scoped));
    let store: Arc<dyn ObjectStore> = store;
    let catalog_path = Path::from("catalog");
    let branch_root = Path::from("branches");
    let segment_pool = Path::from("segment-pool");
    let source_path = Path::from("source");
    let volume_id = Uuid::new_v4();
    let started = Instant::now();
    println!(
        "{}",
        serde_json::json!({
            "event": "cow_stress_started",
            "qualification_id": qualification_id,
            "retained_object_prefix": retained_prefix_text,
            "automatic_cleanup": false,
            "branches": branches,
            "references": references,
            "segments": segments,
            "candidates": candidates,
            "concurrency": concurrency,
            "postgres_tls": postgres_tls,
        })
    );

    let source_db = Db::builder(source_path.clone(), Arc::clone(&store))
        .with_settings(wal_off_settings())
        .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
        .build()
        .await
        .unwrap();
    println!("{}", serde_json::json!({"event": "source_db_open"}));
    let key_codec = KeyCodec::new();
    let mut source_batch = WriteBatch::new();
    for index in 0..references {
        let segid = Segid::new(1, (index % segments) as u64);
        source_batch.put(
            key_codec.extent_key(index as u64 + 1, 0),
            FrameLoc {
                segid,
                frame_index: 0,
                byte_offset: 0,
                byte_len: 1,
            }
            .encode(),
        );
    }
    source_db
        .write_with_options(
            source_batch,
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    println!("{}", serde_json::json!({"event": "source_batch_written"}));
    source_db.flush().await.unwrap();
    println!("{}", serde_json::json!({"event": "source_flushed"}));
    let source_checkpoint = source_db
        .create_checkpoint(
            CheckpointScope::All,
            &CheckpointOptions {
                lifetime: None,
                source: None,
                name: Some("stress-source".to_string()),
            },
        )
        .await
        .unwrap();
    println!("{}", serde_json::json!({"event": "source_checkpointed"}));
    source_db.close().await.unwrap();
    println!("{}", serde_json::json!({"event": "source_db_closed"}));
    let physical = AdminBuilder::new(source_path.clone(), Arc::clone(&store))
        .build()
        .list_checkpoints(Some("stress-source"))
        .await
        .unwrap()
        .into_iter()
        .find(|checkpoint| checkpoint.id == source_checkpoint.id)
        .unwrap();
    let source = ImmutableCheckpoint {
        database_path: source_path,
        checkpoint_id: source_checkpoint.id,
        manifest_id: source_checkpoint.manifest_id,
    };
    let now = catalog_timestamp(physical.create_time);
    let source_branch_id = Uuid::new_v4();

    let catalog = Arc::new(
        SlateDbCatalog::open(catalog_path.clone(), Arc::clone(&store))
            .await
            .unwrap(),
    );
    println!("{}", serde_json::json!({"event": "catalog_open"}));
    catalog
        .apply(CatalogMutation::CreateBranch(BranchRecord {
            id: source_branch_id,
            revision: 1,
            name: "stress-main".to_string(),
            state: BranchState::Ready,
            root: Some(source.durable_root()),
            parent_id: None,
            origin_checkpoint_id: None,
            created_at: now,
            updated_at: now,
        }))
        .await
        .unwrap();
    catalog
        .apply(CatalogMutation::CreateCheckpoint(CheckpointRecord {
            id: source.checkpoint_id,
            revision: 1,
            branch_id: source_branch_id,
            name: "stress-source".to_string(),
            root: source.durable_root(),
            created_at: now,
            updated_at: now,
        }))
        .await
        .unwrap();
    let roots = SlateDbRootStore::new(Arc::clone(&store), branch_root.clone())
        .with_segment_pool_root(segment_pool.clone());
    let lifecycle = Arc::new(BranchLifecycle::new(catalog.clone(), roots));

    let postgres = Arc::new(
        PostgresCatalogProjection::connect_with_tls(&postgres_url, postgres_tls)
            .await
            .unwrap(),
    );
    postgres.migrate().await.unwrap();
    println!("{}", serde_json::json!({"event": "postgres_ready"}));
    let json_directory = tempfile::tempdir().unwrap();
    let json = Arc::new(JsonCatalogProjection::new(
        json_directory.path().join("projection.json"),
    ));
    lifecycle
        .reconcile_projection(volume_id, postgres.as_ref())
        .await
        .unwrap();
    lifecycle
        .reconcile_projection(volume_id, json.as_ref())
        .await
        .unwrap();
    println!("{}", serde_json::json!({"event": "source_ready"}));

    let specs = (0..branches)
        .map(|index| BranchSpec {
            operation_id: Uuid::new_v4(),
            deletion_operation_id: Uuid::new_v4(),
            branch_id: Uuid::new_v4(),
            name: format!("stress-child-{index:04}"),
        })
        .collect::<Vec<_>>();
    stream::iter(specs.clone())
        .map(|spec| {
            let lifecycle = Arc::clone(&lifecycle);
            async move {
                let create = || {
                    lifecycle.create_from_checkpoint_name_by_identity(
                        spec.operation_id,
                        spec.branch_id,
                        spec.name.clone(),
                        source_branch_id,
                        "stress-source".to_string(),
                    )
                };
                let (left, right) = tokio::join!(create(), create());
                assert_eq!(left.unwrap(), right.unwrap());
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
    println!(
        "{}",
        serde_json::json!({"event": "branch_churn_ready", "branches": branches})
    );
    assert!(matches!(
        lifecycle
            .create_from_checkpoint_name_by_identity(
                specs[0].operation_id,
                Uuid::new_v4(),
                "conflicting-rebind".to_string(),
                source_branch_id,
                "stress-source".to_string(),
            )
            .await,
        Err(super::BranchLifecycleError::Catalog(
            CatalogError::OperationConflict(_)
        ))
    ));

    lifecycle
        .reconcile_projection(volume_id, postgres.as_ref())
        .await
        .unwrap();
    lifecycle
        .reconcile_projection(volume_id, json.as_ref())
        .await
        .unwrap();
    for spec in specs.iter().take(branches.min(32)) {
        let mut metadata = CustomerMetadata::new();
        metadata.insert("stress_index".to_string(), Value::String(spec.name.clone()));
        let (left, right) = tokio::join!(
            postgres.set_customer_metadata(volume_id, spec.branch_id, metadata.clone()),
            json.set_customer_metadata(volume_id, spec.branch_id, metadata),
        );
        left.unwrap();
        right.unwrap();
    }
    assert_projection_parity(volume_id, json.as_ref(), postgres.as_ref()).await;

    // The hand-seeded source root intentionally exercises physical checkpoint
    // cloning, but does not have a production root-owner descriptor. Remove it
    // once all descendants have published their independently authenticated
    // roots, so terminal GC validates exactly the roots it could sweep.
    lifecycle
        .delete_checkpoint_by_identity(
            source_branch_id,
            source.checkpoint_id,
            "stress-source".to_string(),
        )
        .await
        .unwrap();
    lifecycle
        .delete_branch_by_identity(Uuid::new_v4(), source_branch_id, "stress-main".to_string())
        .await
        .unwrap();
    lifecycle
        .reconcile_projection(volume_id, postgres.as_ref())
        .await
        .unwrap();
    lifecycle
        .reconcile_projection(volume_id, json.as_ref())
        .await
        .unwrap();
    assert_projection_parity(volume_id, json.as_ref(), postgres.as_ref()).await;

    stream::iter(0..segments + candidates)
        .map(|index| {
            let store = Arc::clone(&store);
            let segment_pool = segment_pool.clone();
            async move {
                let segid = if index < segments {
                    Segid::new(1, index as u64)
                } else {
                    Segid::new(2, (index - segments) as u64)
                };
                store
                    .put(
                        &Path::from(format!("{segment_pool}/{}", segid.object_key())),
                        Bytes::from(vec![(index & 0xff) as u8; 1024]).into(),
                    )
                    .await
                    .unwrap();
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
    println!(
        "{}",
        serde_json::json!({
            "event": "inventory_ready",
            "objects": segments + candidates,
        })
    );

    let writer = lifecycle
        .prepare_server_writer_mount(ServerWriterMountRequest {
            branch_name: specs[0].name.clone(),
            branch_id: specs[0].branch_id,
            server_id: Uuid::new_v4(),
            renewal_secret: Uuid::new_v4(),
            duration: chrono::Duration::minutes(1),
        })
        .await
        .unwrap();
    let blocked_run = Uuid::new_v4();
    assert!(matches!(
        lifecycle.root_captures().begin(blocked_run).await,
        Err(RootCaptureLifecycleError::Catalog(
            CatalogError::WriterLeaseActive(id)
        )) if id == specs[0].branch_id
    ));
    let writer_db = Db::builder(
        Path::from(writer.grant.lease.root.identity.clone()),
        Arc::clone(&store),
    )
    .with_settings(wal_off_settings())
    .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
    .build()
    .await
    .unwrap();
    let mut writer_batch = WriteBatch::new();
    writer_batch.put(KeyCodec::new().inode_key(u64::MAX), b"advanced");
    writer_db
        .write_with_options(
            writer_batch,
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    writer_db.flush().await.unwrap();
    writer_db.close().await.unwrap();
    lifecycle.publish_writer_head(&writer.grant).await.unwrap();
    lifecycle
        .leases()
        .release(
            writer.grant.lease.id,
            writer.grant.lease.revision,
            writer.grant.renewal_token,
        )
        .await
        .unwrap();

    let first_report = run_report(lifecycle.as_ref(), Uuid::new_v4()).await;
    let first_mark = first_report.mark_stats.as_ref().unwrap();
    let first_inventory = first_report.inventory_stats.as_ref().unwrap();
    println!(
        "{}",
        serde_json::json!({
            "event": "rooted_report_observed",
            "mark": first_mark,
            "inventory": first_inventory,
        })
    );
    assert_eq!(first_mark.unique_segments, segments as u64);
    assert_eq!(first_inventory.reachable_objects, segments as u64);
    assert_eq!(first_inventory.candidate_objects, candidates as u64);
    assert_eq!(first_inventory.candidate_bytes, candidates as u64 * 1024);
    println!("{}", serde_json::json!({"event": "rooted_report_ready"}));

    let postgres_reconciler = {
        let lifecycle = Arc::clone(&lifecycle);
        let postgres = Arc::clone(&postgres);
        tokio::spawn(async move {
            for _ in 0..32 {
                lifecycle
                    .reconcile_projection(volume_id, postgres.as_ref())
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
        })
    };
    let json_reconciler = {
        let lifecycle = Arc::clone(&lifecycle);
        let json = Arc::clone(&json);
        tokio::spawn(async move {
            for _ in 0..32 {
                lifecycle
                    .reconcile_projection(volume_id, json.as_ref())
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
        })
    };
    stream::iter(specs.clone())
        .map(|spec| {
            let lifecycle = Arc::clone(&lifecycle);
            async move {
                let delete = || {
                    lifecycle.delete_branch_by_identity(
                        spec.deletion_operation_id,
                        spec.branch_id,
                        spec.name.clone(),
                    )
                };
                let (left, right) = tokio::join!(delete(), delete());
                assert_eq!(left.unwrap(), right.unwrap());
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
    postgres_reconciler.await.unwrap();
    json_reconciler.await.unwrap();
    lifecycle
        .reconcile_projection(volume_id, postgres.as_ref())
        .await
        .unwrap();
    lifecycle
        .reconcile_projection(volume_id, json.as_ref())
        .await
        .unwrap();
    assert_projection_parity(volume_id, json.as_ref(), postgres.as_ref()).await;
    println!("{}", serde_json::json!({"event": "deletion_churn_ready"}));

    lifecycle.close().await.unwrap();
    drop(lifecycle);
    let reopened_catalog = Arc::new(
        SlateDbCatalog::open(catalog_path, Arc::clone(&store))
            .await
            .unwrap(),
    );
    let reopened = BranchLifecycle::new(
        reopened_catalog.clone(),
        SlateDbRootStore::new(Arc::clone(&store), branch_root).with_segment_pool_root(segment_pool),
    );
    let snapshot = reopened_catalog.snapshot().await.unwrap();
    snapshot.validate().unwrap();
    assert!(snapshot.branches.is_empty());
    assert!(snapshot.checkpoints.is_empty());
    assert!(snapshot.leases.is_empty());
    let final_report = run_report(&reopened, Uuid::new_v4()).await;
    let final_inventory = final_report.inventory_stats.as_ref().unwrap();
    assert_eq!(final_inventory.reachable_objects, 0);
    assert_eq!(
        final_inventory.candidate_objects,
        (segments + candidates) as u64
    );
    assert_eq!(
        final_inventory.candidate_bytes,
        (segments + candidates) as u64 * 1024
    );
    reopened.close().await.unwrap();

    println!(
        "{}",
        serde_json::json!({
            "event": "cow_stress_completed",
            "qualification_id": qualification_id,
            "retained_object_prefix": retained_prefix_text,
            "automatic_cleanup": false,
            "elapsed_ms": started.elapsed().as_millis(),
            "branches_created_and_deleted": branches,
            "references_per_root": references,
            "reachable_segments_before_delete": first_inventory.reachable_objects,
            "candidates_before_delete": first_inventory.candidate_objects,
            "candidates_after_delete": final_inventory.candidate_objects,
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
fn stress_guard_requires_s3_and_exact_acknowledgement() {
    assert!(stress_url("s3://stress-bucket/prefix", Some(STRESS_CONFIRMATION)).is_ok());
    for url in [
        "memory:///prefix",
        "file:///tmp/prefix",
        "http://localhost:9000/prefix",
        "s3://user:secret@stress-bucket/prefix",
        "s3://stress-bucket/prefix?secret=value",
        "s3://stress-bucket/prefix#fragment",
    ] {
        assert!(stress_url(url, Some(STRESS_CONFIRMATION)).is_err());
        assert!(branch_bench_url(url, Some(BRANCH_BENCH_CONFIRMATION)).is_err());
    }
    assert!(stress_url("s3://stress-bucket/prefix", None).is_err());
    assert!(branch_bench_url("s3://stress-bucket/prefix", Some(BRANCH_BENCH_CONFIRMATION)).is_ok());
    assert!(branch_bench_url("s3://stress-bucket/prefix", None).is_err());
    assert!(branch_bench_url("s3://stress-bucket/prefix", Some(STRESS_CONFIRMATION)).is_err());
    assert_eq!(percentile(&[1, 2, 3, 4, 5], 50), 3);
    assert_eq!(percentile(&[1, 2, 3, 4, 5], 95), 5);
}
