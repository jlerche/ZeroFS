#!/usr/bin/env bash
set -Eeuo pipefail

# Disposable local qualification for real PostgreSQL data directories on COW
# ZeroFS branches. One process owns the SlateDB catalog and exposes its private
# writer lifecycle over an owner-only Unix socket; every branch worker stays up
# concurrently without independently opening (and fencing) the catalog.

readonly CONFIRMATION="run-disposable-postgres-branches-on-zerofs"
readonly POSTGRES_IMAGE="${ZEROFS_STRESS_POSTGRES_IMAGE:-postgres:17-alpine}"
readonly MINIO_IMAGE="${ZEROFS_STRESS_MINIO_IMAGE:-minio/minio:latest}"
readonly BRANCH_COUNT="${ZEROFS_STRESS_BRANCHES:-3}"
readonly PGBENCH_SCALE="${ZEROFS_STRESS_PGBENCH_SCALE:-2}"
readonly PGBENCH_SECONDS="${ZEROFS_STRESS_PGBENCH_SECONDS:-20}"
readonly PGBENCH_CLIENTS="${ZEROFS_STRESS_PGBENCH_CLIENTS:-4}"
readonly PGBENCH_THREADS="${ZEROFS_STRESS_PGBENCH_THREADS:-2}"
readonly PORT_BASE="${ZEROFS_STRESS_PORT_BASE:-$((19000 + ($$ % 500) * 20))}"
readonly ZEROFS_BIN="${ZEROFS_STRESS_BIN:-$(pwd)/target/debug/zerofs}"
readonly OVERALL_TIMEOUT_SECONDS="${ZEROFS_STRESS_TIMEOUT_SECONDS:-1800}"

if [[ ! "$OVERALL_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "ZEROFS_STRESS_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 2
fi
if [[ "${ZEROFS_STRESS_UNDER_WATCHDOG:-false}" != "true" ]]; then
  command -v timeout >/dev/null || {
    echo "required command is missing: timeout" >&2
    exit 2
  }
  exec timeout --signal=TERM --kill-after=600 "$OVERALL_TIMEOUT_SECONDS" \
    env ZEROFS_STRESS_UNDER_WATCHDOG=true bash "$0" "$@"
fi

if [[ -n "${ZEROFS_STRESS_WORKDIR:-}" ]]; then
  readonly WORK_ROOT="$ZEROFS_STRESS_WORKDIR"
  readonly AUTOMATIC_WORK_ROOT=false
else
  readonly WORK_ROOT="$(mktemp -d -t zerofs-pg-branches.XXXXXX)"
  readonly AUTOMATIC_WORK_ROOT=true
fi

if [[ "${ZEROFS_STRESS_CONFIRM:-}" != "$CONFIRMATION" ]]; then
  echo "ZEROFS_STRESS_CONFIRM must equal $CONFIRMATION" >&2
  exit 2
fi
if (( BRANCH_COUNT < 2 || BRANCH_COUNT > 8 )); then
  echo "ZEROFS_STRESS_BRANCHES must be within 2..=8" >&2
  exit 2
fi
for command in docker mc mountpoint sudo timeout; do
  command -v "$command" >/dev/null || {
    echo "required command is missing: $command" >&2
    exit 2
  }
done
[[ -x "$ZEROFS_BIN" ]] || {
  echo "ZeroFS binary is missing: $ZEROFS_BIN (run cargo build first)" >&2
  exit 2
}
sudo -n true >/dev/null || {
  echo "passwordless sudo is required for isolated --access all FUSE mounts" >&2
  exit 2
}

readonly RUN_ID="$(tr -d '-' </proc/sys/kernel/random/uuid)"
readonly MINIO_NAME="zerofs-pg-minio-${RUN_ID}"
readonly CATALOG_PG_NAME="zerofs-pg-catalog-${RUN_ID}"
readonly MINIO_PORT="$PORT_BASE"
readonly CATALOG_PG_PORT="$((PORT_BASE + 1))"
readonly MINIO_USER="zerofs"
readonly MINIO_PASSWORD="zerofs-local-stress-secret"
readonly PG_PASSWORD="zerofs-local-postgres-secret"
readonly BUCKET="zerofs-stress"
readonly OBJECT_PREFIX="runs/$RUN_ID"
readonly AUTHORITY_DIR="/tmp/zerofs-ca-$RUN_ID"
readonly AUTHORITY_SOCKET="$AUTHORITY_DIR/catalog-writer-authority.sock"
readonly VOLUME_ID="$(</proc/sys/kernel/random/uuid)"
readonly MAIN_BRANCH_ID="$(</proc/sys/kernel/random/uuid)"
readonly MAIN_OPERATION_ID="$(</proc/sys/kernel/random/uuid)"

declare -a CONTAINERS=("$MINIO_NAME" "$CATALOG_PG_NAME")
declare -a DATA_CONTAINERS=()
declare -a ZEROFS_PIDS=()
declare -a MOUNT_PIDS=()
declare -a MOUNTS=()
declare -A OWNED_PIDS=()
declare -a BRANCH_IDS=()
declare -a BRANCH_NAMES=()
declare -a BRANCH_CONFIGS=()
declare -a BRANCH_MOUNTS=()
declare -a BRANCH_PG_NAMES=()
declare -a BRANCH_PG_PORTS=()
declare -a BRANCH_ZEROFS_PIDS=()
declare -a BRANCH_MOUNT_PIDS=()
declare -a PGBENCH_PIDS=()
LAUNCH_SIGNAL_STATUS=0

mkdir -p "$WORK_ROOT"/{cache,configs,logs,mnt,minio}
mkdir -m 0700 "$AUTHORITY_DIR"

event() {
  printf '{"event":"%s","run_id":"%s","work_root":"%s"}\n' "$1" "$RUN_ID" "$WORK_ROOT"
}

begin_atomic_launch() {
  LAUNCH_SIGNAL_STATUS=0
  trap 'LAUNCH_SIGNAL_STATUS=130' INT
  trap 'LAUNCH_SIGNAL_STATUS=143' TERM
}

end_atomic_launch() {
  trap 'exit 130' INT
  trap 'exit 143' TERM
  if (( LAUNCH_SIGNAL_STATUS != 0 )); then
    exit "$LAUNCH_SIGNAL_STATUS"
  fi
}

wait_tcp() {
  local host="$1" port="$2" label="$3"
  for _ in $(seq 1 240); do
    if (exec 9<>"/dev/tcp/$host/$port") 2>/dev/null; then
      exec 9>&-
      return 0
    fi
    sleep 0.25
  done
  echo "timed out waiting for $label on $host:$port" >&2
  return 1
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
    sleep 0.25
  done
  echo "timed out waiting for PostgreSQL container $container" >&2
  docker logs "$container" >&2 || true
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

stop_pid() {
  local pid="$1"
  local expected="${OWNED_PIDS[$pid]:-}"
  [[ -n "$expected" ]] || return 0
  if ! kill -0 "$pid" 2>/dev/null; then
    unset 'OWNED_PIDS[$pid]'
    return 0
  fi
  local command_line
  command_line="$(tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null || true)"
  if [[ "$command_line" != *"$expected"* ]]; then
    unset 'OWNED_PIDS[$pid]'
    echo "refusing to signal PID $pid because its command identity changed" >&2
    return 1
  fi
  kill "$pid" 2>/dev/null || true
  for _ in $(seq 1 40); do
    if ! kill -0 "$pid" 2>/dev/null; then
      unset 'OWNED_PIDS[$pid]'
      return 0
    fi
    sleep 0.25
  done
  kill -9 "$pid" 2>/dev/null || true
  unset 'OWNED_PIDS[$pid]'
}

kill_owned_pid_hard() {
  local pid="$1"
  local expected="${OWNED_PIDS[$pid]:-}"
  [[ -n "$expected" ]] || return 1
  local command_line
  command_line="$(tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null || true)"
  if [[ "$command_line" != *"$expected"* ]]; then
    echo "refusing hard kill of PID $pid because its command identity changed" >&2
    return 1
  fi
  kill -9 "$pid"
  wait "$pid" 2>/dev/null || true
  unset 'OWNED_PIDS[$pid]'
}

unmount_path() {
  local path="$1"
  if mountpoint -q "$path"; then
    sudo fusermount3 -u "$path" 2>/dev/null || sudo fusermount3 -uz "$path" 2>/dev/null || true
  fi
  for _ in $(seq 1 40); do
    mountpoint -q "$path" || return 0
    sleep 0.25
  done
  if mountpoint -q "$path"; then
    echo "failed to unmount $path" >&2
    return 1
  fi
}

remove_container() {
  local container="$1"
  if timeout 30 docker rm -f "$container" >/dev/null 2>&1; then
    return 0
  fi
  local container_pid inspect_status inspect_output
  if inspect_output="$(timeout 10 docker inspect --format '{{.State.Pid}}' "$container" 2>&1)"; then
    inspect_status=0
    container_pid="$inspect_output"
  else
    inspect_status=$?
  fi
  if (( inspect_status != 0 )); then
    (( inspect_status == 124 )) && return 1
    if [[ "$inspect_output" == *"No such object"* ]] \
      || [[ "$inspect_output" == *"No such container"* ]]; then
      return 0
    fi
    echo "failed to inspect cleanup container $container: $inspect_output" >&2
    return 1
  fi
  if [[ "$container_pid" =~ ^[1-9][0-9]*$ ]]; then
    sudo kill -9 "$container_pid" 2>/dev/null || true
  fi
  timeout 30 docker rm -f "$container" >/dev/null 2>&1
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  set +e
  local cleanup_failed=0
  for container in "${CONTAINERS[@]}"; do
    if (( status != 0 )); then
      timeout 10 docker logs "$container" >"$WORK_ROOT/logs/container-$container.log" 2>&1 || true
    fi
  done
  for pid in "${PGBENCH_PIDS[@]}"; do
    [[ -n "$pid" ]] && stop_pid "$pid" || cleanup_failed=1
  done
  for container in "${DATA_CONTAINERS[@]}"; do
    remove_container "$container" || cleanup_failed=1
  done
  for mount in "${MOUNTS[@]}"; do
    unmount_path "$mount" || cleanup_failed=1
  done
  for pid in "${MOUNT_PIDS[@]}" "${ZEROFS_PIDS[@]}"; do
    [[ -n "$pid" ]] && stop_pid "$pid" || cleanup_failed=1
  done
  rm -f -- "$AUTHORITY_SOCKET" || cleanup_failed=1
  rmdir -- "$AUTHORITY_DIR" || cleanup_failed=1
  for container in "$CATALOG_PG_NAME" "$MINIO_NAME"; do
    remove_container "$container" || cleanup_failed=1
  done
  for mount in "${MOUNTS[@]}"; do
    if mountpoint -q "$mount"; then
      echo "refusing workspace deletion because $mount is still mounted" >&2
      cleanup_failed=1
    fi
  done
  mc alias remove "zerofs-$RUN_ID" >/dev/null 2>&1 || true
  if (( status == 0 && cleanup_failed == 0 )) && [[ "$AUTOMATIC_WORK_ROOT" == "true" ]] \
    && [[ "$(basename "$WORK_ROOT")" == zerofs-pg-branches.* ]] \
    && [[ "${ZEROFS_STRESS_KEEP_WORKDIR:-false}" != "true" ]]; then
    sudo rm -rf -- "$WORK_ROOT"
  else
    echo "retained stress workspace: $WORK_ROOT" >&2
  fi
  if (( cleanup_failed != 0 && status == 0 )); then
    status=1
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

write_config() {
  local path="$1" database_path="$2" cache_name="$3" ninep_port="$4" rpc_port="$5"
  local branch_name="${6:-}" branch_id="${7:-}" server_id="${8:-}" renewal_secret="${9:-}"
  local authority_mode="${10:-}"
  {
    echo '[cache]'
    echo "dir = \"$WORK_ROOT/cache/$cache_name\""
    echo 'disk_size_gb = 1.0'
    echo 'memory_size_gb = 0.25'
    echo
    echo '[storage]'
    echo "url = \"s3://$BUCKET/$OBJECT_PREFIX/$database_path\""
    echo 'encryption_password = "local-stress-encryption-password"'
    echo "segment_pool_path = \"$OBJECT_PREFIX/segment-pool\""
    echo
    echo '[filesystem]'
    echo 'max_size_gb = 8.0'
    echo 'ignore_fsync = false'
    echo
    echo '[servers.ninep]'
    echo "addresses = [\"127.0.0.1:$ninep_port\"]"
    echo
    echo '[servers.rpc]'
    echo "addresses = [\"127.0.0.1:$rpc_port\"]"
    echo
    echo '[telemetry]'
    echo 'enabled = false'
    echo
    echo '[aws]'
    echo "access_key_id = \"$MINIO_USER\""
    echo "secret_access_key = \"$MINIO_PASSWORD\""
    echo "endpoint = \"http://127.0.0.1:$MINIO_PORT\""
    echo 'allow_http = "true"'
    echo 'copy_if_not_exists = "multipart"'
    if [[ -n "$branch_name" ]]; then
      echo
      echo '[catalog]'
      echo "volume_id = \"$VOLUME_ID\""
      echo "branch_database_root = \"$OBJECT_PREFIX/branches\""
      echo
      echo '[catalog.authority]'
      echo "slatedb_path = \"$OBJECT_PREFIX/catalog\""
      echo
      echo '[catalog.authority.features]'
      echo 'create = true'
      echo 'mount = true'
      echo 'checkpoint_delete = true'
      echo 'branch_delete = true'
      echo
      echo '[catalog.projection]'
      echo 'backend = "postgres"'
      echo "connection_string = \"postgresql://postgres:$PG_PASSWORD@127.0.0.1:$CATALOG_PG_PORT/catalog\""
      echo 'tls = false'
      if [[ "$authority_mode" == "listen" ]]; then
        echo
        echo '[catalog.writer_authority]'
        echo "listen_unix_socket = \"$AUTHORITY_SOCKET\""
      elif [[ "$authority_mode" == "connect" ]]; then
        echo
        echo '[catalog.writer_authority]'
        echo "connect_unix_socket = \"$AUTHORITY_SOCKET\""
      fi
      if [[ "$branch_name" != "__bootstrap_only__" ]]; then
        echo
        echo '[catalog.mount]'
        echo "branch_name = \"$branch_name\""
        echo "expected_branch_id = \"$branch_id\""
        echo "server_id = \"$server_id\""
        echo "renewal_secret = \"$renewal_secret\""
        echo 'lease_duration_seconds = 120'
      fi
    fi
  } >"$path"
}

start_zerofs() {
  local config="$1" log="$2"
  begin_atomic_launch
  (trap - INT TERM; exec "$ZEROFS_BIN" run --config "$config") >"$log" 2>&1 &
  STARTED_ZEROFS_PID=$!
  OWNED_PIDS[$STARTED_ZEROFS_PID]="$WORK_ROOT"
  ZEROFS_PIDS+=("$STARTED_ZEROFS_PID")
  end_atomic_launch
}

mount_zerofs() {
  local ninep_port="$1" path="$2" log="$3"
  mkdir -p "$path"
  begin_atomic_launch
  (trap - INT TERM; exec sudo "$ZEROFS_BIN" mount --access all --writeback false \
    --relaxed-consistency false "127.0.0.1:$ninep_port" "$path") >"$log" 2>&1 &
  STARTED_MOUNT_PID=$!
  OWNED_PIDS[$STARTED_MOUNT_PID]="$WORK_ROOT"
  MOUNT_PIDS+=("$STARTED_MOUNT_PID")
  MOUNTS+=("$path")
  end_atomic_launch
  wait_mount "$path"
}

start_data_postgres() {
  local name="$1" mount="$2" port="$3"
  CONTAINERS+=("$name")
  DATA_CONTAINERS+=("$name")
  docker run -d --name "$name" --shm-size=256m \
    -e POSTGRES_PASSWORD="$PG_PASSWORD" -e PGDATA=/zerofs/pgdata \
    -p "127.0.0.1:$port:5432" -v "$mount:/zerofs" \
    "$POSTGRES_IMAGE" >/dev/null
  wait_postgres "$name"
}

event services_starting
docker run -d --name "$MINIO_NAME" \
  -e MINIO_ROOT_USER="$MINIO_USER" -e MINIO_ROOT_PASSWORD="$MINIO_PASSWORD" \
  -p "127.0.0.1:$MINIO_PORT:9000" -v "$WORK_ROOT/minio:/data" \
  "$MINIO_IMAGE" server /data >/dev/null
wait_tcp 127.0.0.1 "$MINIO_PORT" MinIO
for _ in $(seq 1 120); do
  if mc alias set "zerofs-$RUN_ID" "http://127.0.0.1:$MINIO_PORT" \
    "$MINIO_USER" "$MINIO_PASSWORD" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done
mc admin info "zerofs-$RUN_ID" >/dev/null
mc mb "zerofs-$RUN_ID/$BUCKET" >/dev/null

docker run -d --name "$CATALOG_PG_NAME" --shm-size=256m \
  -e POSTGRES_PASSWORD="$PG_PASSWORD" -e POSTGRES_DB=catalog \
  -p "127.0.0.1:$CATALOG_PG_PORT:5432" "$POSTGRES_IMAGE" >/dev/null
wait_postgres "$CATALOG_PG_NAME"
event services_ready

readonly BASE_CONFIG="$WORK_ROOT/configs/base.toml"
readonly BASE_MOUNT="$WORK_ROOT/mnt/base"
readonly BASE_NINEP_PORT="$((PORT_BASE + 2))"
readonly BASE_RPC_PORT="$((PORT_BASE + 3))"
readonly BASE_PG_NAME="zerofs-pg-base-${RUN_ID}"
readonly BASE_PG_PORT="$((PORT_BASE + 4))"
write_config "$BASE_CONFIG" source base "$BASE_NINEP_PORT" "$BASE_RPC_PORT"
start_zerofs "$BASE_CONFIG" "$WORK_ROOT/logs/zerofs-base.log"
BASE_ZEROFS_PID=$STARTED_ZEROFS_PID
wait_tcp 127.0.0.1 "$BASE_NINEP_PORT" 'base ZeroFS 9P'
wait_tcp 127.0.0.1 "$BASE_RPC_PORT" 'base ZeroFS RPC'
mount_zerofs "$BASE_NINEP_PORT" "$BASE_MOUNT" "$WORK_ROOT/logs/mount-base.log"
BASE_MOUNT_PID=$STARTED_MOUNT_PID
sudo chmod 0777 "$BASE_MOUNT"
start_data_postgres "$BASE_PG_NAME" "$BASE_MOUNT" "$BASE_PG_PORT"
docker exec "$BASE_PG_NAME" pgbench -i -s "$PGBENCH_SCALE" -U postgres postgres >/dev/null
docker exec "$BASE_PG_NAME" psql -v ON_ERROR_STOP=1 -U postgres postgres -c \
  "CREATE TABLE zerofs_baseline (id integer PRIMARY KEY, value text NOT NULL); INSERT INTO zerofs_baseline VALUES (1, 'shared-before-fork'); CHECKPOINT;" >/dev/null
"$ZEROFS_BIN" checkpoint create --config "$BASE_CONFIG" bootstrap-source >/dev/null
docker stop -t 30 "$BASE_PG_NAME" >/dev/null
unmount_path "$BASE_MOUNT"
stop_pid "$BASE_MOUNT_PID"
stop_pid "$BASE_ZEROFS_PID"
event base_checkpoint_ready

readonly BOOTSTRAP_CONFIG="$WORK_ROOT/configs/bootstrap.toml"
write_config "$BOOTSTRAP_CONFIG" source bootstrap "$((PORT_BASE + 5))" "$((PORT_BASE + 6))" __bootstrap_only__
"$ZEROFS_BIN" branch bootstrap --config "$BOOTSTRAP_CONFIG" main \
  --source-checkpoint bootstrap-source --id "$MAIN_BRANCH_ID" \
  --operation-id "$MAIN_OPERATION_ID" --confirm-offline >"$WORK_ROOT/logs/bootstrap.log"
event catalog_bootstrapped

readonly MAIN_CONFIG="$WORK_ROOT/configs/main.toml"
readonly MAIN_MOUNT="$WORK_ROOT/mnt/main"
readonly MAIN_NINEP_PORT="$((PORT_BASE + 7))"
readonly MAIN_RPC_PORT="$((PORT_BASE + 8))"
readonly MAIN_PG_NAME="zerofs-pg-main-${RUN_ID}"
readonly MAIN_PG_PORT="$((PORT_BASE + 9))"
write_config "$MAIN_CONFIG" source main "$MAIN_NINEP_PORT" "$MAIN_RPC_PORT" main "$MAIN_BRANCH_ID" \
  "$(</proc/sys/kernel/random/uuid)" "$(</proc/sys/kernel/random/uuid)" listen
start_zerofs "$MAIN_CONFIG" "$WORK_ROOT/logs/zerofs-main.log"
MAIN_ZEROFS_PID=$STARTED_ZEROFS_PID
wait_tcp 127.0.0.1 "$MAIN_NINEP_PORT" 'main ZeroFS 9P'
wait_tcp 127.0.0.1 "$MAIN_RPC_PORT" 'main ZeroFS RPC'
mount_zerofs "$MAIN_NINEP_PORT" "$MAIN_MOUNT" "$WORK_ROOT/logs/mount-main.log"
MAIN_MOUNT_PID=$STARTED_MOUNT_PID
start_data_postgres "$MAIN_PG_NAME" "$MAIN_MOUNT" "$MAIN_PG_PORT"
docker exec "$MAIN_PG_NAME" psql -v ON_ERROR_STOP=1 -U postgres postgres -c 'CHECKPOINT;' >/dev/null
"$ZEROFS_BIN" checkpoint create --config "$MAIN_CONFIG" fanout >/dev/null
event fanout_checkpoint_ready

for ((index = 0; index < BRANCH_COUNT; index++)); do
  branch_id="$(</proc/sys/kernel/random/uuid)"
  operation_id="$(</proc/sys/kernel/random/uuid)"
  branch_name="stress-$index"
  BRANCH_IDS+=("$branch_id")
  BRANCH_NAMES+=("$branch_name")
  "$ZEROFS_BIN" branch create --config "$MAIN_CONFIG" "$branch_name" \
    --source-branch-id "$MAIN_BRANCH_ID" --source-checkpoint fanout \
    --id "$branch_id" --operation-id "$operation_id" >"$WORK_ROOT/logs/create-$index.log"
done
event branches_created
[[ -S "$AUTHORITY_SOCKET" ]] || {
  echo "catalog writer authority socket was not created: $AUTHORITY_SOCKET" >&2
  exit 1
}
[[ "$(stat -c '%a' "$AUTHORITY_SOCKET")" == "600" ]] || {
  echo "catalog writer authority socket is not owner-only" >&2
  exit 1
}
event catalog_authority_ready

for ((index = 0; index < BRANCH_COUNT; index++)); do
  ninep_port="$((PORT_BASE + 20 + index * 3))"
  rpc_port="$((ninep_port + 1))"
  pg_port="$((ninep_port + 2))"
  config="$WORK_ROOT/configs/branch-$index.toml"
  mount="$WORK_ROOT/mnt/branch-$index"
  pg_name="zerofs-pg-branch-${index}-${RUN_ID}"
  write_config "$config" source "branch-$index" "$ninep_port" "$rpc_port" \
    "${BRANCH_NAMES[$index]}" "${BRANCH_IDS[$index]}" \
    "$(</proc/sys/kernel/random/uuid)" "$(</proc/sys/kernel/random/uuid)" connect
  BRANCH_CONFIGS+=("$config")
  BRANCH_MOUNTS+=("$mount")
  BRANCH_PG_NAMES+=("$pg_name")
  BRANCH_PG_PORTS+=("$pg_port")
  start_zerofs "$config" "$WORK_ROOT/logs/zerofs-branch-$index.log"
  BRANCH_ZEROFS_PIDS+=("$STARTED_ZEROFS_PID")
  wait_tcp 127.0.0.1 "$ninep_port" "branch $index ZeroFS 9P"
  wait_tcp 127.0.0.1 "$rpc_port" "branch $index ZeroFS RPC"
  mount_zerofs "$ninep_port" "$mount" "$WORK_ROOT/logs/mount-branch-$index.log"
  BRANCH_MOUNT_PIDS+=("$STARTED_MOUNT_PID")
  start_data_postgres "$pg_name" "$mount" "$pg_port"
  docker exec "$pg_name" psql -v ON_ERROR_STOP=1 -U postgres postgres -c \
    "CREATE TABLE zerofs_branch_identity (token text PRIMARY KEY); INSERT INTO zerofs_branch_identity VALUES ('${BRANCH_IDS[$index]}'); UPDATE pgbench_accounts SET abalance = abalance + $((index + 1)) WHERE aid % $BRANCH_COUNT = $index;" >/dev/null
  event branch_postgres_ready
done

event all_branch_postgres_concurrent
REMOTE_CHECKPOINT_OUTPUT="$("$ZEROFS_BIN" checkpoint create \
  --config "${BRANCH_CONFIGS[0]}" remote-authority-roundtrip)"
REMOTE_CHECKPOINT_ID="$(sed -n 's/^  ID: //p' <<<"$REMOTE_CHECKPOINT_OUTPUT")"
[[ "$REMOTE_CHECKPOINT_ID" =~ ^[0-9a-fA-F-]{36}$ ]] || {
  echo 'remote authority checkpoint create did not return a UUID' >&2
  exit 1
}
"$ZEROFS_BIN" checkpoint info --config "${BRANCH_CONFIGS[0]}" \
  remote-authority-roundtrip --id "$REMOTE_CHECKPOINT_ID" \
  | grep -F "ID: $REMOTE_CHECKPOINT_ID" >/dev/null
"$ZEROFS_BIN" checkpoint delete --config "${BRANCH_CONFIGS[0]}" \
  remote-authority-roundtrip --id "$REMOTE_CHECKPOINT_ID" >/dev/null
event remote_checkpoint_lifecycle_verified

for ((index = 0; index < BRANCH_COUNT; index++)); do
  pg_name="${BRANCH_PG_NAMES[$index]}"
  begin_atomic_launch
  (trap - INT TERM; exec timeout "$((PGBENCH_SECONDS + 120))" \
    docker exec "$pg_name" pgbench -T "$PGBENCH_SECONDS" \
      -c "$PGBENCH_CLIENTS" -j "$PGBENCH_THREADS" -U postgres postgres) \
      >"$WORK_ROOT/logs/pgbench-$index.log" 2>&1 &
  pgbench_pid="$!"
  OWNED_PIDS[$pgbench_pid]="$pg_name"
  PGBENCH_PIDS+=("$pgbench_pid")
  end_atomic_launch
done
pgbench_status=0
for pid in "${PGBENCH_PIDS[@]}"; do
  if ! wait "$pid"; then
    pgbench_status=1
  fi
  unset 'OWNED_PIDS[$pid]'
done
PGBENCH_PIDS=()
if (( pgbench_status != 0 )); then
  echo 'one or more concurrent pgbench jobs failed' >&2
  exit 1
fi
event concurrent_divergent_load_complete

# Crash the sole catalog owner while every child PostgreSQL/ZeroFS pair stays
# live. Restart it with the same deterministic main capability, then wait longer
# than a worker renewal interval so every existing gRPC channel must prove it
# can renew through the replacement authority.
docker stop -t 30 "$MAIN_PG_NAME" >/dev/null
unmount_path "$MAIN_MOUNT"
stop_pid "$MAIN_MOUNT_PID"
kill_owned_pid_hard "$MAIN_ZEROFS_PID"
start_zerofs "$MAIN_CONFIG" "$WORK_ROOT/logs/zerofs-main-authority-restart.log"
MAIN_ZEROFS_PID=$STARTED_ZEROFS_PID
wait_tcp 127.0.0.1 "$MAIN_NINEP_PORT" 'restarted catalog authority ZeroFS 9P'
wait_tcp 127.0.0.1 "$MAIN_RPC_PORT" 'restarted catalog authority ZeroFS RPC'
for _ in $(seq 1 120); do
  [[ -S "$AUTHORITY_SOCKET" ]] && break
  sleep 0.25
done
[[ -S "$AUTHORITY_SOCKET" ]] || {
  echo 'catalog writer authority socket did not recover after hard restart' >&2
  exit 1
}
mount_zerofs "$MAIN_NINEP_PORT" "$MAIN_MOUNT" "$WORK_ROOT/logs/mount-main-authority-restart.log"
MAIN_MOUNT_PID=$STARTED_MOUNT_PID
docker start "$MAIN_PG_NAME" >/dev/null
wait_postgres "$MAIN_PG_NAME"
sleep 45
for ((index = 0; index < BRANCH_COUNT; index++)); do
  kill -0 "${BRANCH_ZEROFS_PIDS[$index]}" 2>/dev/null || {
    echo "branch $index stopped after catalog authority restart" >&2
    exit 1
  }
  wait_tcp 127.0.0.1 "$((PORT_BASE + 20 + index * 3))" \
    "branch $index after catalog authority renewal"
done
event catalog_authority_crash_recovered

for ((index = 0; index < BRANCH_COUNT; index++)); do
  pg_name="${BRANCH_PG_NAMES[$index]}"
  mount="${BRANCH_MOUNTS[$index]}"
  config="${BRANCH_CONFIGS[$index]}"
  ninep_port="$((PORT_BASE + 20 + index * 3))"
  rpc_port="$((ninep_port + 1))"
  if (( index == 0 )); then
    docker kill "$pg_name" >/dev/null
    docker start "$pg_name" >/dev/null
    wait_postgres "$pg_name"
    event postgres_crash_recovered
  fi
  if (( index == 1 )); then
    docker stop -t 30 "$pg_name" >/dev/null
    unmount_path "$mount"
    stop_pid "${BRANCH_MOUNT_PIDS[$index]}"
    kill_owned_pid_hard "${BRANCH_ZEROFS_PIDS[$index]}"
    start_zerofs "$config" "$WORK_ROOT/logs/zerofs-branch-$index-restart.log"
    BRANCH_ZEROFS_PIDS[$index]="$STARTED_ZEROFS_PID"
    wait_tcp 127.0.0.1 "$ninep_port" 'restarted branch ZeroFS 9P'
    wait_tcp 127.0.0.1 "$rpc_port" 'restarted branch ZeroFS RPC'
    mount_zerofs "$ninep_port" "$mount" "$WORK_ROOT/logs/mount-branch-$index-restart.log"
    BRANCH_MOUNT_PIDS[$index]="$STARTED_MOUNT_PID"
    docker start "$pg_name" >/dev/null
    wait_postgres "$pg_name"
    event zerofs_crash_recovered
  fi

  docker exec "$pg_name" psql -At -v ON_ERROR_STOP=1 -U postgres postgres -c \
    "SELECT value FROM zerofs_baseline WHERE id = 1;" | grep -Fx 'shared-before-fork' >/dev/null
  docker exec "$pg_name" psql -At -v ON_ERROR_STOP=1 -U postgres postgres -c \
    "SELECT token FROM zerofs_branch_identity;" | grep -Fx "${BRANCH_IDS[$index]}" >/dev/null
  docker exec "$pg_name" pg_amcheck --all --install-missing -U postgres >/dev/null
done

if docker exec "$MAIN_PG_NAME" psql -At -v ON_ERROR_STOP=1 -U postgres postgres -c \
  "SELECT to_regclass('public.zerofs_branch_identity') IS NULL;" | grep -Fx t >/dev/null; then
  :
else
  echo 'main branch observed child-only table' >&2
  exit 1
fi
event integrity_and_isolation_verified

printf '{"event":"stress_complete","run_id":"%s","branches":%d,"pgbench_seconds":%d,"work_root":"%s"}\n' \
  "$RUN_ID" "$BRANCH_COUNT" "$PGBENCH_SECONDS" "$WORK_ROOT"
