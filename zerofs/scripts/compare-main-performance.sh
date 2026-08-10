#!/usr/bin/env bash
set -Eeuo pipefail

# Reproducible A/B filesystem comparison between an exact main ref and the
# current checkout. Each sample uses a fresh MinIO prefix and ZeroFS cache.

readonly CONFIRMATION="run-disposable-main-performance-comparison"
readonly MAIN_REF="${ZEROFS_PERF_MAIN_REF:-origin/main}"
readonly TRIALS="${ZEROFS_PERF_TRIALS:-3}"
readonly MINIO_IMAGE="${ZEROFS_PERF_MINIO_IMAGE:-minio/minio:latest}"
readonly POSTGRES_IMAGE="${ZEROFS_PERF_POSTGRES_IMAGE:-postgres:17-alpine}"
readonly PGBENCH_SCALE="${ZEROFS_PERF_PGBENCH_SCALE:-2}"
readonly PGBENCH_SECONDS="${ZEROFS_PERF_PGBENCH_SECONDS:-10}"
readonly PGBENCH_CLIENTS="${ZEROFS_PERF_PGBENCH_CLIENTS:-4}"
readonly PGBENCH_THREADS="${ZEROFS_PERF_PGBENCH_THREADS:-2}"
readonly IGNORE_FSYNC="${ZEROFS_PERF_IGNORE_FSYNC:-false}"
readonly PORT_BASE="${ZEROFS_PERF_PORT_BASE:-$((23000 + ($$ % 500) * 10))}"
readonly OVERALL_TIMEOUT_SECONDS="${ZEROFS_PERF_TIMEOUT_SECONDS:-3600}"
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly CANDIDATE_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD)"
readonly CANDIDATE_TREE_HASH="$(git -C "$REPO_ROOT" write-tree)"
readonly CANDIDATE_REVISION="tree:$CANDIDATE_TREE_HASH"
readonly MAIN_REVISION="$(git -C "$REPO_ROOT" rev-parse "$MAIN_REF")"
readonly PERF_RUN_ID="$(tr -d '-' </proc/sys/kernel/random/uuid)"

if [[ "${ZEROFS_PERF_CONFIRM:-}" != "$CONFIRMATION" ]]; then
  echo "ZEROFS_PERF_CONFIRM must equal $CONFIRMATION" >&2
  exit 2
fi
if [[ ! "$OVERALL_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "ZEROFS_PERF_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 2
fi
if [[ "${ZEROFS_PERF_UNDER_WATCHDOG:-false}" != "true" ]]; then
  command -v timeout >/dev/null || {
    echo "required command is missing: timeout" >&2
    exit 2
  }
  # Cleanup's independently bounded serial worst case is 425 seconds. Preserve
  # enough grace for every resource class to be attempted before hard kill.
  exec timeout --signal=TERM --kill-after=600 "$OVERALL_TIMEOUT_SECONDS" \
    env ZEROFS_PERF_UNDER_WATCHDOG=true bash "$0" "$@"
fi
if [[ ! "$TRIALS" =~ ^[1-9][0-9]*$ || "$TRIALS" -gt 20 ]]; then
  echo "ZEROFS_PERF_TRIALS must be within 1..=20" >&2
  exit 2
fi
for value in "$PGBENCH_SCALE" "$PGBENCH_SECONDS" "$PGBENCH_CLIENTS" "$PGBENCH_THREADS"; do
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "pgbench scale, seconds, clients, and threads must be positive integers" >&2
    exit 2
  fi
done
if (( PGBENCH_THREADS > PGBENCH_CLIENTS )); then
  echo "ZEROFS_PERF_PGBENCH_THREADS cannot exceed ZEROFS_PERF_PGBENCH_CLIENTS" >&2
  exit 2
fi
if [[ "$IGNORE_FSYNC" != "true" && "$IGNORE_FSYNC" != "false" ]]; then
  echo "ZEROFS_PERF_IGNORE_FSYNC must be true or false" >&2
  exit 2
fi
if [[ "$CANDIDATE_COMMIT" == "$MAIN_REVISION" ]]; then
  echo "candidate and main resolve to the same commit" >&2
  exit 2
fi
for command in cargo docker env flock git jq mc mountpoint nc rustc sha256sum sort sudo; do
  command -v "$command" >/dev/null || {
    echo "required command is missing: $command" >&2
    exit 2
  }
done
sudo -n true >/dev/null || {
  echo "passwordless sudo is required for isolated --access all FUSE mounts" >&2
  exit 2
}
readonly RUSTC_VERSION="$(rustc -Vv)"
readonly CARGO_VERSION="$(cargo -V)"
readonly BUILD_ENVIRONMENT_SHA256="$(
  {
    printf 'rustc=%s\n' "$RUSTC_VERSION"
    printf 'cargo=%s\n' "$CARGO_VERSION"
    env -0 | LC_ALL=C sort -z
  } | sha256sum | awk '{print $1}'
)"

if [[ -n "${ZEROFS_PERF_WORKDIR:-}" ]]; then
  readonly WORK_ROOT="$ZEROFS_PERF_WORKDIR"
  mkdir -p "$WORK_ROOT"
else
  readonly WORK_ROOT="$(mktemp -d -t zerofs-perf.XXXXXX)"
fi
readonly MAIN_TREE="$WORK_ROOT/main-worktree-${PERF_RUN_ID:0:16}"
readonly CANDIDATE_TREE="$WORK_ROOT/candidate-worktree-${PERF_RUN_ID:0:16}"
readonly RESULTS_DIR="$WORK_ROOT/results"
readonly LOGS_DIR="$WORK_ROOT/logs"
readonly MINIO_DATA="$WORK_ROOT/minio"
readonly RUN_TOKEN="${PERF_RUN_ID:0:16}"
readonly CONTAINER_RUN_LABEL="com.zerofs.performance-run=$PERF_RUN_ID"
readonly MINIO_NAME="zerofs-perf-minio-$RUN_TOKEN"
readonly MINIO_PORT="$PORT_BASE"
readonly MINIO_USER="zerofs"
readonly MINIO_PASSWORD="zerofs-local-perf-secret"
readonly BUCKET="zerofs-perf"
readonly ALIAS="zerofs-perf-$RUN_TOKEN"

mkdir -p "$RESULTS_DIR" "$LOGS_DIR" "$MINIO_DATA" "$WORK_ROOT/bin"
exec 9>"$WORK_ROOT/.performance.lock"
if ! flock -n 9; then
  echo "performance workspace is already in use: $WORK_ROOT" >&2
  exit 2
fi

ACTIVE_ZEROFS_PID=""
ACTIVE_MOUNT_PID=""
ACTIVE_MOUNT=""
ACTIVE_PG_CONTAINER=""
WORKTREE_ADDED=false
CANDIDATE_WORKTREE_ADDED=false
ALIAS_CONFIGURED=false
MAIN_BINARY_SHA256=""
CANDIDATE_BINARY_SHA256=""
BENCH_BINARY_SHA256=""
MINIO_IMAGE_ID=""
POSTGRES_IMAGE_ID=""
SIGNAL_PENDING=0
SIGNAL_RESTORE_TEST_HOOK=false

begin_launch_registration() {
  SIGNAL_PENDING=0
  trap 'SIGNAL_PENDING=130' INT
  trap 'SIGNAL_PENDING=143' TERM
}

finish_launch_registration() {
  trap 'exit 130' INT
  trap 'exit 143' TERM
  if [[ "$SIGNAL_RESTORE_TEST_HOOK" == "true" ]]; then
    kill -TERM "$BASHPID"
  fi
  local pending="$SIGNAL_PENDING"
  SIGNAL_PENDING=0
  if (( pending != 0 )); then
    exit "$pending"
  fi
}

test_launch_signal_boundaries() {
  local status
  if (
    trap - EXIT
    begin_launch_registration
    kill -TERM "$BASHPID"
    finish_launch_registration
    exit 99
  ); then
    status=0
  else
    status=$?
  fi
  if (( status != 143 )); then
    echo "deferred launch signal self-test failed with status $status" >&2
    return 1
  fi
  if (
    trap - EXIT
    SIGNAL_RESTORE_TEST_HOOK=true
    begin_launch_registration
    finish_launch_registration
    exit 99
  ); then
    status=0
  else
    status=$?
  fi
  if (( status != 143 )); then
    echo "signal-restore boundary self-test failed with status $status" >&2
    return 1
  fi
}

is_mounted() {
  local status
  if timeout 5 mountpoint -q "$1"; then
    return 0
  else
    status=$?
  fi
  case "$status" in
    32) return 1 ;;
    *)
      echo "failed to inspect mount state for $1 (status $status)" >&2
      return 2
      ;;
  esac
}

remove_owned_container() {
  local name="$1" listed label
  if ! listed="$(timeout 15 docker container ls -a \
    --filter "name=^/${name}$" --format '{{.Names}}')"; then
    echo "failed to inspect Docker container $name" >&2
    return 1
  fi
  if [[ -z "$listed" ]]; then
    return 0
  fi
  if [[ "$listed" != "$name" ]]; then
    echo "Docker name filter for $name returned an unexpected container: $listed" >&2
    return 1
  fi
  if ! label="$(timeout 15 docker inspect --format \
    '{{ index .Config.Labels "com.zerofs.performance-run" }}' "$name")"; then
    echo "failed to read ownership label for Docker container $name" >&2
    return 1
  fi
  if [[ "$label" != "$PERF_RUN_ID" ]]; then
    echo "refusing to remove foreign Docker container $name" >&2
    return 1
  fi
  if ! timeout 30 docker rm -f "$name" >/dev/null; then
    echo "failed to remove owned Docker container $name" >&2
    return 1
  fi
  if ! listed="$(timeout 15 docker container ls -a --filter "name=^/${name}$" \
    --format '{{.Names}}')"; then
    echo "failed to verify removal of Docker container $name" >&2
    return 1
  fi
  if [[ -n "$listed" ]]; then
    echo "owned Docker container $name still exists after removal" >&2
    return 1
  fi
}

stop_owned_pid() {
  local pid="$1" expected="$2" label="$3" command
  if ! kill -0 "$pid" 2>/dev/null; then
    wait "$pid" 2>/dev/null || true
    return 0
  fi
  command="$(tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null || true)"
  if [[ "$command" != *"$expected"* ]]; then
    echo "refusing to signal reused $label PID $pid" >&2
    return 1
  fi
  kill "$pid" 2>/dev/null || true
  for _ in $(seq 1 100); do
    kill -0 "$pid" 2>/dev/null || break
    [[ "$(awk '{print $3}' "/proc/$pid/stat" 2>/dev/null || true)" == "Z" ]] && break
    sleep 0.1
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill -KILL "$pid" 2>/dev/null || true
  fi
  for _ in $(seq 1 100); do
    kill -0 "$pid" 2>/dev/null || break
    [[ "$(awk '{print $3}' "/proc/$pid/stat" 2>/dev/null || true)" == "Z" ]] && break
    sleep 0.1
  done
  local state
  state="$(awk '{print $3}' "/proc/$pid/stat" 2>/dev/null || true)"
  if kill -0 "$pid" 2>/dev/null && [[ "$state" != "Z" ]]; then
    echo "$label PID $pid is still alive after bounded shutdown" >&2
    return 1
  fi
  wait "$pid" 2>/dev/null || true
}

remove_build_worktree() {
  local path="$1" registered
  if ! registered="$(timeout 15 git -C "$REPO_ROOT" worktree list --porcelain \
    | awk -v target="$path" '$1 == "worktree" && $2 == target {print $2}')"; then
    echo "failed to inspect Git worktree registration for $path" >&2
    return 1
  fi
  if [[ "$registered" == "$path" ]]; then
    if ! timeout 60 git -C "$REPO_ROOT" worktree remove --force "$path" >/dev/null; then
      echo "failed to remove performance build worktree $path" >&2
      return 1
    fi
  elif [[ -e "$path" ]]; then
    echo "refusing to remove unregistered path at worktree location $path" >&2
    return 1
  fi
}

stop_active_sample() {
  local failed=0
  if [[ -n "$ACTIVE_PG_CONTAINER" ]]; then
    if remove_owned_container "$ACTIVE_PG_CONTAINER"; then
      ACTIVE_PG_CONTAINER=""
    else
      failed=1
    fi
  fi
  if [[ -n "$ACTIVE_MOUNT" ]]; then
    if is_mounted "$ACTIVE_MOUNT"; then
      timeout 30 sudo fusermount3 -u "$ACTIVE_MOUNT" 2>/dev/null \
        || timeout 30 sudo fusermount3 -uz "$ACTIVE_MOUNT" 2>/dev/null \
        || failed=1
    elif (( $? != 1 )); then
      failed=1
    fi
  fi
  if [[ -n "$ACTIVE_MOUNT_PID" ]]; then
    if stop_owned_pid "$ACTIVE_MOUNT_PID" "$ACTIVE_MOUNT" "mount"; then
      ACTIVE_MOUNT_PID=""
    else
      failed=1
    fi
  fi
  if [[ -n "$ACTIVE_MOUNT" ]]; then
    if is_mounted "$ACTIVE_MOUNT"; then
      echo "FUSE mount $ACTIVE_MOUNT remains active after bounded shutdown" >&2
      failed=1
    elif (( $? == 1 )); then
      ACTIVE_MOUNT=""
    else
      failed=1
    fi
  fi
  if [[ -n "$ACTIVE_ZEROFS_PID" ]]; then
    if stop_owned_pid "$ACTIVE_ZEROFS_PID" "$WORK_ROOT/bin/" "ZeroFS"; then
      ACTIVE_ZEROFS_PID=""
    else
      failed=1
    fi
  fi
  return "$failed"
}

wait_postgres() {
  local container="$1"
  local ready_count=0
  for _ in $(seq 1 240); do
    if docker exec "$container" pg_isready -U postgres >/dev/null 2>&1; then
      ready_count=$((ready_count + 1))
      if (( ready_count >= 8 )); then
        return 0
      fi
    else
      ready_count=0
    fi
    if ! docker inspect "$container" >/dev/null 2>&1; then
      echo "PostgreSQL container $container exited during startup" >&2
      return 1
    fi
    sleep 0.25
  done
  echo "timed out waiting for PostgreSQL container $container" >&2
  docker logs "$container" >&2 || true
  return 1
}

cleanup() {
  local status=$?
  local cleanup_failed=0
  trap - EXIT INT TERM
  set +e
  stop_active_sample || cleanup_failed=1
  remove_owned_container "$MINIO_NAME" || cleanup_failed=1
  if [[ "$ALIAS_CONFIGURED" == "true" ]]; then
    if timeout 15 mc alias remove "$ALIAS" >/dev/null; then
      ALIAS_CONFIGURED=false
    else
      echo "failed to remove MinIO client alias $ALIAS" >&2
      cleanup_failed=1
    fi
  fi
  if [[ "$WORKTREE_ADDED" == "true" ]]; then
    remove_build_worktree "$MAIN_TREE" || cleanup_failed=1
  fi
  if [[ "$CANDIDATE_WORKTREE_ADDED" == "true" ]]; then
    remove_build_worktree "$CANDIDATE_TREE" || cleanup_failed=1
  fi
  if (( cleanup_failed != 0 && status == 0 )); then
    status=1
  fi
  echo "performance evidence retained: $WORK_ROOT" >&2
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
test_launch_signal_boundaries
if [[ "${ZEROFS_PERF_SIGNAL_SELF_TEST_ONLY:-false}" == "true" ]]; then
  exit 0
fi

wait_tcp() {
  local port="$1" label="$2"
  for _ in $(seq 1 240); do
    nc -z 127.0.0.1 "$port" >/dev/null 2>&1 && return 0
    if [[ -n "$ACTIVE_ZEROFS_PID" ]] && ! kill -0 "$ACTIVE_ZEROFS_PID" 2>/dev/null; then
      echo "$label exited before opening port $port" >&2
      return 1
    fi
    sleep 0.25
  done
  echo "timed out waiting for $label on port $port" >&2
  return 1
}

wait_mount() {
  local path="$1"
  for _ in $(seq 1 120); do
    mountpoint -q "$path" && return 0
    sleep 0.25
  done
  echo "timed out waiting for FUSE mount $path" >&2
  return 1
}

write_config() {
  local path="$1" cache="$2" object_prefix="$3" ninep_port="$4" rpc_port="$5"
  cat >"$path" <<EOF
[cache]
dir = "$cache"
disk_size_gb = 2.0
memory_size_gb = 1.0

[storage]
url = "s3://$BUCKET/$object_prefix"
encryption_password = "local-performance-encryption-password"

[filesystem]
max_size_gb = 16.0
ignore_fsync = $IGNORE_FSYNC

[servers.ninep]
addresses = ["127.0.0.1:$ninep_port"]

[servers.rpc]
addresses = ["127.0.0.1:$rpc_port"]

[telemetry]
enabled = false

[aws]
access_key_id = "$MINIO_USER"
secret_access_key = "$MINIO_PASSWORD"
endpoint = "http://127.0.0.1:$MINIO_PORT"
allow_http = "true"
copy_if_not_exists = "multipart"
EOF
}

build_binaries() {
  local candidate_revision_file="$WORK_ROOT/bin/candidate.revision"
  local candidate_environment_file="$WORK_ROOT/bin/candidate.build-environment"
  local bench_revision_file="$WORK_ROOT/bin/bench.revision"
  local bench_environment_file="$WORK_ROOT/bin/bench.build-environment"
  local main_revision_file="$WORK_ROOT/bin/main.revision"
  local main_environment_file="$WORK_ROOT/bin/main.build-environment"
  local candidate_build_commit=""
  local need_candidate_tree=false
  if [[ ! -x "$WORK_ROOT/bin/candidate" \
    || ! -f "$candidate_revision_file" \
    || "$(<"$candidate_revision_file")" != "$CANDIDATE_REVISION" \
    || ! -f "$candidate_environment_file" \
    || "$(<"$candidate_environment_file")" != "$BUILD_ENVIRONMENT_SHA256" ]]; then
    need_candidate_tree=true
  fi
  if [[ ! -x "$WORK_ROOT/bin/bench" \
    || ! -f "$bench_revision_file" \
    || "$(<"$bench_revision_file")" != "$CANDIDATE_REVISION" \
    || ! -f "$bench_environment_file" \
    || "$(<"$bench_environment_file")" != "$BUILD_ENVIRONMENT_SHA256" ]]; then
    need_candidate_tree=true
  fi
  if [[ "$need_candidate_tree" == "true" ]]; then
    candidate_build_commit="$(
      GIT_AUTHOR_NAME='ZeroFS performance harness' \
      GIT_AUTHOR_EMAIL='performance-harness@invalid' \
      GIT_COMMITTER_NAME='ZeroFS performance harness' \
      GIT_COMMITTER_EMAIL='performance-harness@invalid' \
      git -C "$REPO_ROOT" commit-tree "$CANDIDATE_TREE_HASH" \
        -p "$CANDIDATE_COMMIT" <<<"Exact staged performance candidate"
    )"
    CANDIDATE_WORKTREE_ADDED=true
    git -C "$REPO_ROOT" worktree add --detach "$CANDIDATE_TREE" \
      "$candidate_build_commit"
  fi
  if [[ ! -x "$WORK_ROOT/bin/candidate" \
    || ! -f "$candidate_revision_file" \
    || "$(<"$candidate_revision_file")" != "$CANDIDATE_REVISION" \
    || ! -f "$candidate_environment_file" \
    || "$(<"$candidate_environment_file")" != "$BUILD_ENVIRONMENT_SHA256" ]]; then
    echo "building candidate $CANDIDATE_REVISION" >&2
    (cd "$CANDIDATE_TREE/zerofs" && cargo build --release)
    cp "$CANDIDATE_TREE/zerofs/target/release/zerofs" "$WORK_ROOT/bin/candidate"
    printf '%s\n' "$CANDIDATE_REVISION" >"$candidate_revision_file"
    printf '%s\n' "$BUILD_ENVIRONMENT_SHA256" >"$candidate_environment_file"
  fi
  if [[ ! -x "$WORK_ROOT/bin/bench" \
    || ! -f "$bench_revision_file" \
    || "$(<"$bench_revision_file")" != "$CANDIDATE_REVISION" \
    || ! -f "$bench_environment_file" \
    || "$(<"$bench_environment_file")" != "$BUILD_ENVIRONMENT_SHA256" ]]; then
    (cd "$CANDIDATE_TREE/bench" && cargo build --release)
    cp "$CANDIDATE_TREE/bench/target/release/bench" "$WORK_ROOT/bin/bench"
    printf '%s\n' "$CANDIDATE_REVISION" >"$bench_revision_file"
    printf '%s\n' "$BUILD_ENVIRONMENT_SHA256" >"$bench_environment_file"
  fi
  if [[ "$CANDIDATE_WORKTREE_ADDED" == "true" ]]; then
    remove_build_worktree "$CANDIDATE_TREE"
    CANDIDATE_WORKTREE_ADDED=false
  fi

  if [[ ! -x "$WORK_ROOT/bin/main" \
    || ! -f "$main_revision_file" \
    || "$(<"$main_revision_file")" != "$MAIN_REVISION" \
    || ! -f "$main_environment_file" \
    || "$(<"$main_environment_file")" != "$BUILD_ENVIRONMENT_SHA256" ]]; then
    WORKTREE_ADDED=true
    git -C "$REPO_ROOT" worktree add --detach "$MAIN_TREE" "$MAIN_REVISION"
    echo "building main $MAIN_REVISION" >&2
    (cd "$MAIN_TREE/zerofs" && cargo build --release)
    cp "$MAIN_TREE/zerofs/target/release/zerofs" "$WORK_ROOT/bin/main"
    printf '%s\n' "$MAIN_REVISION" >"$main_revision_file"
    printf '%s\n' "$BUILD_ENVIRONMENT_SHA256" >"$main_environment_file"
    remove_build_worktree "$MAIN_TREE"
    WORKTREE_ADDED=false
  fi
  MAIN_BINARY_SHA256="$(sha256sum "$WORK_ROOT/bin/main" | awk '{print $1}')"
  CANDIDATE_BINARY_SHA256="$(sha256sum "$WORK_ROOT/bin/candidate" | awk '{print $1}')"
  BENCH_BINARY_SHA256="$(sha256sum "$WORK_ROOT/bin/bench" | awk '{print $1}')"
}

run_benchmark() {
  local variant="$1" trial="$2" benchmark="$3" ops="$4" size="$5" mount="$6"
  local raw="$RESULTS_DIR/${trial}-${variant}-${benchmark}.raw"
  local json="$RESULTS_DIR/${trial}-${variant}-${benchmark}.json"
  "$WORK_ROOT/bin/bench" run --work-dir "$mount/bench-$benchmark" \
    --ops "$ops" --size "$size" --benchmark "$benchmark" --format json \
    >"$raw" 2>"$LOGS_DIR/${trial}-${variant}-${benchmark}.stderr"
  sed -n '/^\[/,$p' "$raw" >"$json"
  jq -e 'length == 1 and .[0].failed_ops == 0' "$json" >/dev/null
  jq -c --arg variant "$variant" --argjson trial "$trial" \
    '.[0] + {variant: $variant, trial: $trial}' "$json" \
    >>"$RESULTS_DIR/samples.jsonl"
}

run_pgbench() {
  local variant="$1" trial="$2" sequence="$3" mount="$4"
  local pg_root="$mount/postgres"
  local log="$LOGS_DIR/${trial}-${variant}-pgbench.log"
  local container="zerofs-perf-pg-$RUN_TOKEN-$sequence-$variant"
  mkdir -p "$pg_root"
  chmod 0777 "$pg_root"
  ACTIVE_PG_CONTAINER="$container"
  if ! docker run -d --name "$container" --label "$CONTAINER_RUN_LABEL" \
    --shm-size=256m -e POSTGRES_PASSWORD=zerofs-local-performance-secret \
    -e PGDATA=/zerofs/pgdata -v "$pg_root:/zerofs" "$POSTGRES_IMAGE_ID" \
    >"$LOGS_DIR/${trial}-${variant}-pg-container-id"; then
    remove_owned_container "$container" || true
    return 1
  fi
  wait_postgres "$ACTIVE_PG_CONTAINER"
  docker exec "$ACTIVE_PG_CONTAINER" pgbench -i -s "$PGBENCH_SCALE" \
    -U postgres postgres >"$LOGS_DIR/${trial}-${variant}-pgbench-init.log" 2>&1
  docker exec "$ACTIVE_PG_CONTAINER" psql -v ON_ERROR_STOP=1 -U postgres postgres \
    -c 'CHECKPOINT' >/dev/null
  docker exec "$ACTIVE_PG_CONTAINER" pgbench -T "$PGBENCH_SECONDS" \
    -c "$PGBENCH_CLIENTS" -j "$PGBENCH_THREADS" -U postgres postgres >"$log" 2>&1
  local tps transactions
  # pgbench reports both connection-inclusive and steady-state TPS. Use the
  # final, connection-exclusive value so one-time connection setup does not
  # dominate a short storage-engine comparison.
  tps="$(awk '/^tps = / {value=$3} END {print value}' "$log")"
  transactions="$(awk '/^number of transactions actually processed:/ {print $6; exit}' "$log")"
  if [[ ! "$tps" =~ ^[0-9]+([.][0-9]+)?$ || ! "$transactions" =~ ^[0-9]+$ ]]; then
    echo "failed to parse pgbench result for $variant trial $trial" >&2
    sed -n '1,200p' "$log" >&2
    return 1
  fi
  jq -cn --arg variant "$variant" --argjson trial "$trial" \
    --argjson tps "$tps" --argjson transactions "$transactions" \
    '{name:"postgres-pgbench", variant:$variant, trial:$trial,
      ops_per_second:$tps, total_ops:$transactions, successful_ops:$transactions,
      failed_ops:0}' >>"$RESULTS_DIR/samples.jsonl"
  if remove_owned_container "$ACTIVE_PG_CONTAINER"; then
    ACTIVE_PG_CONTAINER=""
  else
    return 1
  fi
}

run_sample() {
  local variant="$1" trial="$2" sequence="$3"
  local binary="$WORK_ROOT/bin/$variant"
  local sample="$WORK_ROOT/run-$PERF_RUN_ID-sample-$sequence-$variant"
  local config="$sample/config.toml"
  local mount="$sample/mount"
  local cache="$sample/cache"
  local ninep_port="$((PORT_BASE + 1))"
  local rpc_port="$((PORT_BASE + 2))"
  mkdir -p "$mount" "$cache"
  write_config "$config" "$cache" "runs/$PERF_RUN_ID/samples/$sequence-$variant" \
    "$ninep_port" "$rpc_port"

  begin_launch_registration
  (
    trap - INT TERM
    exec "$binary" run --config "$config"
  ) >"$LOGS_DIR/${trial}-${variant}-zerofs.log" 2>&1 &
  ACTIVE_ZEROFS_PID=$!
  finish_launch_registration
  wait_tcp "$ninep_port" "$variant ZeroFS"
  ACTIVE_MOUNT="$mount"
  begin_launch_registration
  (
    trap - INT TERM
    exec sudo "$binary" mount --access all --writeback false \
      --relaxed-consistency false "127.0.0.1:$ninep_port" "$mount"
  ) >"$LOGS_DIR/${trial}-${variant}-mount.log" 2>&1 &
  ACTIVE_MOUNT_PID=$!
  finish_launch_registration
  wait_mount "$mount"
  sudo chmod 0777 "$mount"

  # Untimed warm-up establishes connections and JIT-free steady state.
  "$WORK_ROOT/bin/bench" run --work-dir "$mount/warmup" --ops 16 --size 65536 \
    --benchmark sequential-writes --format json \
    >"$LOGS_DIR/${trial}-${variant}-warmup.log" 2>&1
  sync
  sleep 1

  run_benchmark "$variant" "$trial" sequential-writes 256 262144 "$mount"
  run_benchmark "$variant" "$trial" single-file-append 256 262144 "$mount"
  run_benchmark "$variant" "$trial" random-reads 2000 65536 "$mount"
  run_benchmark "$variant" "$trial" metadata-ops 2000 4096 "$mount"
  run_benchmark "$variant" "$trial" empty-files 2000 4096 "$mount"
  run_pgbench "$variant" "$trial" "$sequence" "$mount"
  stop_active_sample
}

summarize() {
  jq -s \
    --arg main_revision "$MAIN_REVISION" \
    --arg candidate_revision "$CANDIDATE_REVISION" \
    --arg candidate_commit "$CANDIDATE_COMMIT" \
    --arg main_binary_sha256 "$MAIN_BINARY_SHA256" \
    --arg candidate_binary_sha256 "$CANDIDATE_BINARY_SHA256" \
    --arg bench_binary_sha256 "$BENCH_BINARY_SHA256" \
    --arg harness_sha256 "$(sha256sum "$0" | awk '{print $1}')" \
    --arg minio_image "$MINIO_IMAGE" \
    --arg minio_image_id "$MINIO_IMAGE_ID" \
    --arg postgres_image "$POSTGRES_IMAGE" \
    --arg postgres_image_id "$POSTGRES_IMAGE_ID" \
    --arg rustc_version "$RUSTC_VERSION" \
    --arg cargo_version "$CARGO_VERSION" \
    --arg build_environment_sha256 "$BUILD_ENVIRONMENT_SHA256" \
    --arg run_id "$PERF_RUN_ID" \
    --argjson pgbench_scale "$PGBENCH_SCALE" \
    --argjson pgbench_seconds "$PGBENCH_SECONDS" \
    --argjson pgbench_clients "$PGBENCH_CLIENTS" \
    --argjson pgbench_threads "$PGBENCH_THREADS" \
    --argjson ignore_fsync "$IGNORE_FSYNC" '
    def median:
      sort as $values
      | ($values | length) as $n
      | if $n % 2 == 1 then $values[$n / 2 | floor]
        else (($values[$n / 2 - 1] + $values[$n / 2]) / 2)
        end;
    def stats($variant; $name):
      [.[] | select(.variant == $variant and .name == $name) | .ops_per_second]
      | {samples: length, median_ops_per_second: median, min_ops_per_second: min,
         max_ops_per_second: max};
    . as $all
    | ([.[].name] | unique) as $names
    | {
        main_revision: $main_revision,
        candidate_revision: $candidate_revision,
        candidate_commit: $candidate_commit,
        artifacts: {
          main_binary_sha256: $main_binary_sha256,
          candidate_binary_sha256: $candidate_binary_sha256,
          bench_binary_sha256: $bench_binary_sha256,
          harness_sha256: $harness_sha256,
          minio_image: $minio_image,
          minio_image_id: $minio_image_id,
          postgres_image: $postgres_image,
          postgres_image_id: $postgres_image_id,
          rustc_version: $rustc_version,
          cargo_version: $cargo_version,
          build_environment_sha256: $build_environment_sha256
        },
        run_id: $run_id,
        trials: ([.[].trial] | unique | length),
        pgbench: {scale: $pgbench_scale, seconds: $pgbench_seconds,
          clients: $pgbench_clients, threads: $pgbench_threads},
        ignore_fsync: $ignore_fsync,
        results: [$names[] as $name
          | (stats("main"; $name)) as $main
          | (stats("candidate"; $name)) as $candidate
          | ([
               $all[]
               | select(.variant == "candidate" and .name == $name) as $candidate_sample
               | $all[]
               | select(.variant == "main" and .name == $name and
                   .trial == $candidate_sample.trial)
               | ((($candidate_sample.ops_per_second / .ops_per_second) - 1) * 100)
             ]) as $paired
          | {benchmark: $name, main: $main, candidate: $candidate,
             paired_delta_percent: $paired,
             paired_median_delta_percent: ($paired | median),
             paired_mean_delta_percent: (($paired | add) / ($paired | length)),
             delta_percent: ((($candidate.median_ops_per_second /
               $main.median_ops_per_second) - 1) * 100)}]
      }
  ' "$RESULTS_DIR/samples.jsonl" >"$RESULTS_DIR/summary.json"
  jq . "$RESULTS_DIR/summary.json"
}

build_binaries
docker pull "$MINIO_IMAGE" >"$LOGS_DIR/minio-image-pull.log"
docker pull "$POSTGRES_IMAGE" >"$LOGS_DIR/postgres-image-pull.log"
MINIO_IMAGE_ID="$(docker image inspect --format '{{.Id}}' "$MINIO_IMAGE")"
POSTGRES_IMAGE_ID="$(docker image inspect --format '{{.Id}}' "$POSTGRES_IMAGE")"
if ! docker run -d --name "$MINIO_NAME" --label "$CONTAINER_RUN_LABEL" \
  -e MINIO_ROOT_USER="$MINIO_USER" -e MINIO_ROOT_PASSWORD="$MINIO_PASSWORD" \
  -p "127.0.0.1:$MINIO_PORT:9000" -v "$MINIO_DATA:/data" \
  "$MINIO_IMAGE_ID" server /data >"$LOGS_DIR/minio-container-id"; then
  remove_owned_container "$MINIO_NAME" || true
  exit 1
fi
MINIO_READY=false
for _ in $(seq 1 120); do
  if mc alias set "$ALIAS" "http://127.0.0.1:$MINIO_PORT" \
    "$MINIO_USER" "$MINIO_PASSWORD" >/dev/null 2>&1; then
    ALIAS_CONFIGURED=true
    MINIO_READY=true
    break
  fi
  sleep 0.25
done
if [[ "$MINIO_READY" != "true" ]]; then
  echo "timed out configuring MinIO alias $ALIAS" >&2
  docker logs "$MINIO_NAME" >&2 || true
  exit 1
fi
mc admin info "$ALIAS" >/dev/null
mc mb --ignore-existing "$ALIAS/$BUCKET" >/dev/null

rm -f -- "$RESULTS_DIR/samples.jsonl" "$RESULTS_DIR/summary.json"
sequence=0
for trial in $(seq 1 "$TRIALS"); do
  if (( trial % 2 == 1 )); then
    order=(main candidate)
  else
    order=(candidate main)
  fi
  for variant in "${order[@]}"; do
    sequence=$((sequence + 1))
    echo "trial $trial/$TRIALS: $variant" >&2
    run_sample "$variant" "$trial" "$sequence"
  done
done
summarize
