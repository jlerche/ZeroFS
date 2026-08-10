use crate::cli::CatalogRuntime;
use crate::config::ServerBranchMountConfig;
use crate::rpc::authority_proto::{
    self,
    catalog_writer_authority_service_client::CatalogWriterAuthorityServiceClient,
    catalog_writer_authority_service_server::{
        CatalogWriterAuthorityService, CatalogWriterAuthorityServiceServer,
    },
};
use anyhow::{Context, Result};
use hyper_util::rt::TokioIo;
use std::path::{Path, PathBuf};
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint, Uri};
use tonic::{Request, Response, Status};
use tower::service_fn;

#[derive(Clone)]
pub(crate) struct CatalogWriterAuthorityClient {
    client: CatalogWriterAuthorityServiceClient<Channel>,
}

impl CatalogWriterAuthorityClient {
    pub(crate) async fn connect(socket_path: PathBuf) -> Result<Self> {
        let display_path = socket_path.clone();
        let channel = Endpoint::try_from("http://localhost")?
            .timeout(std::time::Duration::from_secs(5))
            .connect_with_connector(service_fn(move |_: Uri| {
                let socket_path = socket_path.clone();
                async move {
                    connect_verified_unix_socket(&socket_path)
                        .await
                        .map(TokioIo::new)
                }
            }))
            .await
            .with_context(|| {
                format!(
                    "Failed to connect to catalog writer authority at {}",
                    display_path.display()
                )
            })?;
        Ok(Self {
            client: CatalogWriterAuthorityServiceClient::new(channel),
        })
    }

    pub(crate) async fn prepare_writer_mount(
        &self,
        config: &ServerBranchMountConfig,
    ) -> Result<zerofs::catalog::ServerWriterMountPreparation> {
        let response = self
            .client
            .clone()
            .prepare_writer(authority_proto::PrepareWriterRequest {
                branch_name: config.branch_name.clone(),
                expected_branch_id: config.expected_branch_id.to_string(),
                server_id: config.server_id.to_string(),
                renewal_secret: config.renewal_secret.to_string(),
                lease_duration_seconds: config.lease_duration_seconds,
            })
            .await
            .map_err(status_error)?
            .into_inner();
        serde_json::from_slice(&response.preparation_json)
            .context("Catalog authority returned an invalid writer preparation")
    }

    pub(crate) async fn renew_writer_mount(
        &self,
        grant: &zerofs::catalog::LeaseGrant,
        duration: chrono::Duration,
    ) -> Result<zerofs::catalog::LeaseGrant> {
        self.grant_call(grant, duration, GrantCall::Renew).await
    }

    pub(crate) async fn recover_writer_mount(
        &self,
        grant: &zerofs::catalog::LeaseGrant,
        duration: chrono::Duration,
    ) -> Result<zerofs::catalog::LeaseGrant> {
        self.grant_call(grant, duration, GrantCall::Recover).await
    }

    async fn grant_call(
        &self,
        grant: &zerofs::catalog::LeaseGrant,
        duration: chrono::Duration,
        call: GrantCall,
    ) -> Result<zerofs::catalog::LeaseGrant> {
        let lease_duration_seconds = u64::try_from(duration.num_seconds())
            .context("Writer lease duration must be positive")?;
        let request = authority_proto::WriterGrantRequest {
            grant_json: serde_json::to_vec(grant)?,
            lease_duration_seconds,
        };
        let response = match call {
            GrantCall::Renew => self.client.clone().renew_writer(request).await,
            GrantCall::Recover => self.client.clone().recover_writer(request).await,
        }
        .map_err(status_error)?
        .into_inner();
        serde_json::from_slice(&response.grant_json)
            .context("Catalog authority returned an invalid writer grant")
    }

    pub(crate) async fn publish_writer_head(
        &self,
        grant: &zerofs::catalog::LeaseGrant,
    ) -> Result<()> {
        self.client
            .clone()
            .publish_writer(authority_proto::WriterGrant {
                grant_json: serde_json::to_vec(grant)?,
            })
            .await
            .map_err(status_error)?;
        Ok(())
    }

    async fn publish_checkpoint(
        &self,
        request: zerofs::catalog::CheckpointCreateRequest,
    ) -> Result<zerofs::catalog::CheckpointRecord> {
        let response = self
            .client
            .clone()
            .publish_checkpoint(authority_proto::PublishCheckpointRequest {
                checkpoint_id: request.checkpoint_id.to_string(),
                branch_id: request.branch_id.to_string(),
                name: request.name,
                database_path: request.source.database_path.to_string(),
                physical_checkpoint_id: request.source.checkpoint_id.to_string(),
                manifest_id: request.source.manifest_id,
                created_at_seconds: request.created_at.timestamp(),
                created_at_nanos: request.created_at.timestamp_subsec_nanos(),
            })
            .await
            .map_err(status_error)?
            .into_inner();
        serde_json::from_slice(&response.record_json)
            .context("Catalog authority returned an invalid checkpoint record")
    }

    async fn delete_checkpoint(
        &self,
        branch_id: uuid::Uuid,
        checkpoint_id: uuid::Uuid,
        name: String,
    ) -> Result<zerofs::catalog::TombstoneRecord> {
        let response = self
            .client
            .clone()
            .delete_checkpoint(authority_proto::DeleteCheckpointRequest {
                branch_id: branch_id.to_string(),
                checkpoint_id: checkpoint_id.to_string(),
                name,
            })
            .await
            .map_err(status_error)?
            .into_inner();
        serde_json::from_slice(&response.record_json)
            .context("Catalog authority returned an invalid checkpoint tombstone")
    }

    pub(crate) fn checkpoint_catalog(
        &self,
        branch_id: uuid::Uuid,
    ) -> RemoteCheckpointCatalogAuthority {
        RemoteCheckpointCatalogAuthority {
            client: self.clone(),
            branch_id,
        }
    }
}

#[cfg(unix)]
async fn connect_verified_unix_socket(socket_path: &Path) -> std::io::Result<UnixStream> {
    validate_socket_parent(socket_path).map_err(permission_denied)?;
    validate_socket_endpoint(socket_path).map_err(permission_denied)?;
    let stream = UnixStream::connect(socket_path).await?;

    // Authenticate the actual connected peer, not just the filesystem entry.
    // This is deliberately inside the connector so tonic reconnects are
    // subject to the same check before any catalog credential is transmitted.
    let peer_uid = stream.peer_cred()?.uid();
    // SAFETY: geteuid has no preconditions.
    let effective_uid = unsafe { libc::geteuid() };
    validate_peer_uid(peer_uid, effective_uid).map_err(permission_denied)?;
    Ok(stream)
}

#[cfg(not(unix))]
async fn connect_verified_unix_socket(_socket_path: &Path) -> std::io::Result<UnixStream> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "catalog writer authority Unix sockets are unsupported on this platform",
    ))
}

fn permission_denied(error: anyhow::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::PermissionDenied, error.to_string())
}

pub(crate) struct RemoteCheckpointCatalogAuthority {
    client: CatalogWriterAuthorityClient,
    branch_id: uuid::Uuid,
}

#[async_trait::async_trait]
impl crate::rpc::server::CheckpointCatalogAuthority for RemoteCheckpointCatalogAuthority {
    fn branch_id(&self) -> uuid::Uuid {
        self.branch_id
    }

    async fn publish(
        &self,
        request: zerofs::catalog::CheckpointCreateRequest,
    ) -> Result<zerofs::catalog::CheckpointRecord> {
        if request.branch_id != self.branch_id {
            anyhow::bail!("checkpoint request does not match the mounted branch");
        }
        self.client.publish_checkpoint(request).await
    }

    async fn delete(
        &self,
        checkpoint_id: uuid::Uuid,
        name: String,
    ) -> Result<zerofs::catalog::TombstoneRecord> {
        self.client
            .delete_checkpoint(self.branch_id, checkpoint_id, name)
            .await
    }
}

enum GrantCall {
    Renew,
    Recover,
}

fn status_error(status: Status) -> anyhow::Error {
    anyhow::anyhow!(
        "Catalog writer authority request failed: {}",
        status.message()
    )
}

#[derive(Clone)]
struct CatalogWriterAuthorityRpc {
    runtime: CatalogRuntime,
}

fn internal_error(error: anyhow::Error) -> Status {
    tracing::warn!("catalog writer authority operation failed: {error:#}");
    Status::failed_precondition(error.to_string())
}

fn parse_uuid(value: &str, field: &str) -> Result<uuid::Uuid, Status> {
    let id = uuid::Uuid::parse_str(value)
        .map_err(|_| Status::invalid_argument(format!("{field} must be a valid UUID")))?;
    if id.is_nil() {
        return Err(Status::invalid_argument(format!("{field} must not be nil")));
    }
    Ok(id)
}

fn parse_duration(seconds: u64) -> Result<chrono::Duration, Status> {
    if !(1..=300).contains(&seconds) {
        return Err(Status::invalid_argument(
            "lease_duration_seconds must be within 1..=300",
        ));
    }
    Ok(chrono::Duration::seconds(seconds as i64))
}

fn decode_grant(bytes: &[u8]) -> Result<zerofs::catalog::LeaseGrant, Status> {
    serde_json::from_slice(bytes)
        .map_err(|_| Status::invalid_argument("grant_json is not a valid writer grant"))
}

#[tonic::async_trait]
impl CatalogWriterAuthorityService for CatalogWriterAuthorityRpc {
    async fn prepare_writer(
        &self,
        request: Request<authority_proto::PrepareWriterRequest>,
    ) -> Result<Response<authority_proto::WriterPreparation>, Status> {
        let request = request.into_inner();
        let config = ServerBranchMountConfig {
            branch_name: request.branch_name,
            expected_branch_id: parse_uuid(&request.expected_branch_id, "expected_branch_id")?,
            server_id: parse_uuid(&request.server_id, "server_id")?,
            renewal_secret: parse_uuid(&request.renewal_secret, "renewal_secret")?,
            lease_duration_seconds: request.lease_duration_seconds,
        };
        parse_duration(config.lease_duration_seconds)?;
        let preparation = self
            .runtime
            .prepare_writer_mount(&config)
            .await
            .map_err(internal_error)?;
        Ok(Response::new(authority_proto::WriterPreparation {
            preparation_json: serde_json::to_vec(&preparation).map_err(|error| {
                Status::internal(format!("failed to encode writer preparation: {error}"))
            })?,
        }))
    }

    async fn renew_writer(
        &self,
        request: Request<authority_proto::WriterGrantRequest>,
    ) -> Result<Response<authority_proto::WriterGrant>, Status> {
        let request = request.into_inner();
        let grant = decode_grant(&request.grant_json)?;
        let grant = self
            .runtime
            .renew_writer_mount(&grant, parse_duration(request.lease_duration_seconds)?)
            .await
            .map_err(internal_error)?;
        Ok(Response::new(authority_proto::WriterGrant {
            grant_json: serde_json::to_vec(&grant)
                .map_err(|error| Status::internal(format!("failed to encode grant: {error}")))?,
        }))
    }

    async fn recover_writer(
        &self,
        request: Request<authority_proto::WriterGrantRequest>,
    ) -> Result<Response<authority_proto::WriterGrant>, Status> {
        let request = request.into_inner();
        let grant = decode_grant(&request.grant_json)?;
        let grant = self
            .runtime
            .recover_writer_mount(&grant, parse_duration(request.lease_duration_seconds)?)
            .await
            .map_err(internal_error)?;
        Ok(Response::new(authority_proto::WriterGrant {
            grant_json: serde_json::to_vec(&grant)
                .map_err(|error| Status::internal(format!("failed to encode grant: {error}")))?,
        }))
    }

    async fn publish_writer(
        &self,
        request: Request<authority_proto::WriterGrant>,
    ) -> Result<Response<authority_proto::PublishWriterResponse>, Status> {
        let grant = decode_grant(&request.into_inner().grant_json)?;
        self.runtime
            .publish_writer_head(&grant)
            .await
            .map_err(internal_error)?;
        Ok(Response::new(authority_proto::PublishWriterResponse {}))
    }

    async fn publish_checkpoint(
        &self,
        request: Request<authority_proto::PublishCheckpointRequest>,
    ) -> Result<Response<authority_proto::CatalogRecord>, Status> {
        let request = request.into_inner();
        let branch_id = parse_uuid(&request.branch_id, "branch_id")?;
        let created_at = chrono::DateTime::<chrono::Utc>::from_timestamp(
            request.created_at_seconds,
            request.created_at_nanos,
        )
        .ok_or_else(|| Status::invalid_argument("created_at is invalid"))?;
        let record = self
            .runtime
            .checkpoint_catalog(branch_id)
            .publish(zerofs::catalog::CheckpointCreateRequest {
                checkpoint_id: parse_uuid(&request.checkpoint_id, "checkpoint_id")?,
                branch_id,
                name: request.name,
                source: zerofs::catalog::ImmutableCheckpoint {
                    database_path: slatedb::object_store::path::Path::from(request.database_path),
                    checkpoint_id: parse_uuid(
                        &request.physical_checkpoint_id,
                        "physical_checkpoint_id",
                    )?,
                    manifest_id: request.manifest_id,
                },
                created_at,
            })
            .await
            .map_err(internal_error)?;
        Ok(Response::new(authority_proto::CatalogRecord {
            record_json: serde_json::to_vec(&record).map_err(|error| {
                Status::internal(format!("failed to encode checkpoint record: {error}"))
            })?,
        }))
    }

    async fn delete_checkpoint(
        &self,
        request: Request<authority_proto::DeleteCheckpointRequest>,
    ) -> Result<Response<authority_proto::CatalogRecord>, Status> {
        let request = request.into_inner();
        let record = self
            .runtime
            .checkpoint_catalog(parse_uuid(&request.branch_id, "branch_id")?)
            .delete(
                parse_uuid(&request.checkpoint_id, "checkpoint_id")?,
                request.name,
            )
            .await
            .map_err(internal_error)?;
        Ok(Response::new(authority_proto::CatalogRecord {
            record_json: serde_json::to_vec(&record).map_err(|error| {
                Status::internal(format!("failed to encode checkpoint tombstone: {error}"))
            })?,
        }))
    }
}

#[derive(Debug)]
pub(crate) struct CatalogWriterAuthorityListener {
    socket_path: PathBuf,
    listener: UnixListener,
    _authority_lock: std::fs::File,
}

impl CatalogWriterAuthorityListener {
    pub(crate) fn bind(socket_path: PathBuf, volume_id: uuid::Uuid) -> Result<Self> {
        validate_socket_parent(&socket_path)?;
        let lock_path = catalog_authority_lock_path(volume_id)?;
        let authority_lock = acquire_authority_lock(&lock_path, &socket_path)?;
        if socket_path.exists() {
            let metadata = std::fs::symlink_metadata(&socket_path).with_context(|| {
                format!(
                    "Failed to inspect existing catalog authority socket {}",
                    socket_path.display()
                )
            })?;
            if !is_unix_socket(&metadata) {
                anyhow::bail!(
                    "Refusing to replace non-socket catalog authority path {}",
                    socket_path.display()
                );
            }
            std::fs::remove_file(&socket_path).with_context(|| {
                format!(
                    "Failed to remove stale catalog authority socket {}",
                    socket_path.display()
                )
            })?;
        }
        let listener = UnixListener::bind(&socket_path).with_context(|| {
            format!(
                "Failed to bind catalog authority socket {}",
                socket_path.display()
            )
        })?;
        if let Err(error) = set_owner_only_permissions(&socket_path) {
            let _ = std::fs::remove_file(&socket_path);
            return Err(error);
        }
        Ok(Self {
            socket_path,
            listener,
            _authority_lock: authority_lock,
        })
    }
}

pub(crate) async fn serve(
    listener: CatalogWriterAuthorityListener,
    runtime: CatalogRuntime,
    shutdown: CancellationToken,
) -> Result<()> {
    let CatalogWriterAuthorityListener {
        socket_path,
        listener,
        _authority_lock,
    } = listener;
    tracing::info!(
        "Catalog writer authority listening on owner-only Unix socket {}",
        socket_path.display()
    );
    let service = CatalogWriterAuthorityServiceServer::new(CatalogWriterAuthorityRpc { runtime });
    let result = tonic::transport::Server::builder()
        .add_service(service)
        .serve_with_incoming_shutdown(
            UnixListenerStream::new(listener),
            shutdown.cancelled_owned(),
        )
        .await
        .context("Catalog writer authority server failed");
    if let Err(error) = std::fs::remove_file(&socket_path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(
            "failed to remove catalog authority socket {}: {error}",
            socket_path.display()
        );
    }
    result
}

#[cfg(unix)]
fn acquire_authority_lock(lock_path: &Path, socket_path: &Path) -> Result<std::fs::File> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(lock_path)
        .with_context(|| {
            format!(
                "Failed to open catalog authority lock {}",
                lock_path.display()
            )
        })?;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .with_context(|| {
            format!(
                "Failed to protect catalog authority lock {}",
                lock_path.display()
            )
        })?;
    // SAFETY: flock only reads the valid file descriptor and the file remains
    // owned by this process for the complete server lifetime.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        anyhow::bail!(
            "Another catalog writer authority owns {}",
            socket_path.display()
        );
    }
    Ok(file)
}

#[cfg(not(unix))]
fn acquire_authority_lock(_lock_path: &Path, _socket_path: &Path) -> Result<std::fs::File> {
    anyhow::bail!("catalog writer authority Unix sockets are unsupported on this platform")
}

#[cfg(unix)]
fn validate_socket_parent(socket_path: &Path) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let parent = socket_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!("catalog authority socket must have an explicit parent directory")
        })?;
    let metadata = std::fs::symlink_metadata(parent).with_context(|| {
        format!(
            "Failed to inspect catalog authority socket directory {}",
            parent.display()
        )
    })?;
    if !metadata.file_type().is_dir() {
        anyhow::bail!(
            "Catalog authority socket directory {} must be a real directory, not a symlink",
            parent.display()
        );
    }
    let mode = metadata.permissions().mode() & 0o777;
    // SAFETY: geteuid has no preconditions.
    let effective_uid = unsafe { libc::geteuid() };
    if metadata.uid() != effective_uid || mode & 0o077 != 0 {
        anyhow::bail!(
            "Catalog authority socket directory {} must be owned by uid {} with no group/other permissions (mode 0700 recommended)",
            parent.display(),
            effective_uid
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_socket_parent(_socket_path: &Path) -> Result<()> {
    anyhow::bail!("catalog writer authority Unix sockets are unsupported on this platform")
}

#[cfg(unix)]
fn validate_socket_endpoint(socket_path: &Path) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = std::fs::symlink_metadata(socket_path).with_context(|| {
        format!(
            "Failed to inspect catalog authority socket {}",
            socket_path.display()
        )
    })?;
    if !is_unix_socket(&metadata) {
        anyhow::bail!(
            "Catalog authority endpoint {} must be a Unix socket, not a symlink or regular file",
            socket_path.display()
        );
    }
    // SAFETY: geteuid has no preconditions.
    let effective_uid = unsafe { libc::geteuid() };
    let mode = metadata.permissions().mode() & 0o777;
    if metadata.uid() != effective_uid || mode & 0o077 != 0 {
        anyhow::bail!(
            "Catalog authority endpoint {} must be owned by uid {} with no group/other permissions",
            socket_path.display(),
            effective_uid
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_socket_endpoint(_socket_path: &Path) -> Result<()> {
    anyhow::bail!("catalog writer authority Unix sockets are unsupported on this platform")
}

fn validate_peer_uid(peer_uid: libc::uid_t, expected_uid: libc::uid_t) -> Result<()> {
    if peer_uid != expected_uid {
        anyhow::bail!(
            "Catalog authority peer uid {peer_uid} does not match worker uid {expected_uid}"
        );
    }
    Ok(())
}

#[cfg(unix)]
fn catalog_authority_lock_path(volume_id: uuid::Uuid) -> Result<PathBuf> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};

    // SAFETY: geteuid has no preconditions.
    let effective_uid = unsafe { libc::geteuid() };
    // A fixed host path keeps the fence canonical even when two processes use
    // different TMPDIR environments or different authority socket paths.
    let directory = PathBuf::from("/tmp").join(format!("zerofs-catalog-authority-{effective_uid}"));
    let mut builder = std::fs::DirBuilder::new();
    builder.mode(0o700);
    match builder.create(&directory) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "Failed to create catalog authority lock directory {}",
                    directory.display()
                )
            });
        }
    }
    let metadata = std::fs::symlink_metadata(&directory).with_context(|| {
        format!(
            "Failed to inspect catalog authority lock directory {}",
            directory.display()
        )
    })?;
    if !metadata.file_type().is_dir()
        || metadata.uid() != effective_uid
        || metadata.permissions().mode() & 0o077 != 0
    {
        anyhow::bail!(
            "Catalog authority lock directory {} is not an owner-only real directory",
            directory.display()
        );
    }
    Ok(directory.join(format!("{volume_id}.lock")))
}

#[cfg(not(unix))]
fn catalog_authority_lock_path(_volume_id: uuid::Uuid) -> Result<PathBuf> {
    anyhow::bail!("catalog writer authority Unix sockets are unsupported on this platform")
}

#[cfg(unix)]
fn is_unix_socket(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::FileTypeExt;
    metadata.file_type().is_socket()
}

#[cfg(not(unix))]
fn is_unix_socket(_metadata: &std::fs::Metadata) -> bool {
    false
}

#[cfg(unix)]
fn set_owner_only_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).with_context(|| {
        format!(
            "Failed to protect catalog authority socket {}",
            path.display()
        )
    })
}

#[cfg(not(unix))]
fn set_owner_only_permissions(_path: &Path) -> Result<()> {
    anyhow::bail!("catalog writer authority Unix sockets are unsupported on this platform")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn authority_lock_is_exclusive_and_owner_only() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("authority.sock");
        let lock_path = directory.path().join("catalog.lock");
        let first = acquire_authority_lock(&lock_path, &socket).unwrap();
        let error = acquire_authority_lock(&lock_path, &socket).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Another catalog writer authority")
        );
        assert_eq!(
            std::fs::metadata(&lock_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        drop(first);
        acquire_authority_lock(&lock_path, &socket).unwrap();
    }

    #[tokio::test]
    async fn listener_claim_precedes_catalog_open_and_recovers_stale_socket() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = directory.path().join("authority.sock");
        let other_socket = directory.path().join("other-authority.sock");
        let volume_id = uuid::Uuid::new_v4();
        let first = CatalogWriterAuthorityListener::bind(socket.clone(), volume_id).unwrap();
        assert_eq!(
            std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let error =
            CatalogWriterAuthorityListener::bind(other_socket.clone(), volume_id).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Another catalog writer authority")
        );
        drop(first);
        CatalogWriterAuthorityListener::bind(other_socket, volume_id).unwrap();
    }

    #[test]
    fn listener_never_replaces_a_non_socket_path() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = directory.path().join("authority.sock");
        std::fs::write(&socket, b"operator data").unwrap();
        let error =
            CatalogWriterAuthorityListener::bind(socket.clone(), uuid::Uuid::new_v4()).unwrap_err();
        assert!(error.to_string().contains("Refusing to replace non-socket"));
        assert_eq!(std::fs::read(socket).unwrap(), b"operator data");
    }

    #[test]
    fn listener_rejects_a_non_owner_only_parent() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o770)).unwrap();
        let socket = directory.path().join("authority.sock");
        let error = CatalogWriterAuthorityListener::bind(socket, uuid::Uuid::new_v4()).unwrap_err();
        assert!(error.to_string().contains("no group/other permissions"));
    }

    #[tokio::test]
    async fn client_rejects_a_socket_in_an_unsafe_parent() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
        let socket = directory.path().join("fake-authority.sock");
        let _listener = UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();

        let error = connect_verified_unix_socket(&socket).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("no group/other permissions"));
    }

    #[tokio::test]
    async fn client_rejects_an_over_permissive_socket() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = directory.path().join("fake-authority.sock");
        let _listener = UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o666)).unwrap();

        let error = connect_verified_unix_socket(&socket).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("no group/other permissions"));
    }

    #[tokio::test]
    async fn client_rejects_a_non_socket_endpoint() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = directory.path().join("fake-authority.sock");
        std::fs::write(&socket, b"credential sink").unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();

        let error = connect_verified_unix_socket(&socket).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("must be a Unix socket"));
    }

    #[test]
    fn client_rejects_a_peer_from_another_uid() {
        let error = validate_peer_uid(1001, 1000).unwrap_err();
        assert!(error.to_string().contains("does not match worker uid"));
    }
}
