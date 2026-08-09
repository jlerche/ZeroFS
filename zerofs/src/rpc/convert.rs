use crate::checkpoint_manager::CheckpointInfo;
use crate::fs::tracing::{FileAccessEvent, FileOperation};
use crate::object_trace::{ObjectAccessEvent, ObjectOperation};
use crate::rpc::proto;
use prost_types::Timestamp;
use serde_json::Value;
use std::fmt;
use uuid::Uuid;
use zerofs::catalog::{CustomerCatalogRecord, CustomerResourceKind};

impl fmt::Display for proto::FileOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            proto::FileOperation::Read => "read   ",
            proto::FileOperation::Write => "write  ",
            proto::FileOperation::Create => "create ",
            proto::FileOperation::Remove => "remove ",
            proto::FileOperation::Rename => "rename ",
            proto::FileOperation::Mkdir => "mkdir  ",
            proto::FileOperation::Readdir => "readdir",
            proto::FileOperation::Lookup => "lookup ",
            proto::FileOperation::Setattr => "setattr",
            proto::FileOperation::Link => "link   ",
            proto::FileOperation::Symlink => "symlink",
            proto::FileOperation::Mknod => "mknod  ",
            proto::FileOperation::Trim => "trim   ",
            proto::FileOperation::Fsync => "fsync  ",
            proto::FileOperation::Fallocate => "falloc ",
        };
        write!(f, "{}", s)
    }
}

impl fmt::Display for proto::ObjectOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            proto::ObjectOperation::ObjGet => "get   ",
            proto::ObjectOperation::ObjHead => "head  ",
            proto::ObjectOperation::ObjPut => "put   ",
            proto::ObjectOperation::ObjPutMultipart => "mpart ",
            proto::ObjectOperation::ObjDelete => "delete",
            proto::ObjectOperation::ObjList => "list  ",
            proto::ObjectOperation::ObjCopy => "copy  ",
            proto::ObjectOperation::ObjRename => "rename",
        };
        write!(f, "{}", s)
    }
}

impl From<CheckpointInfo> for proto::CheckpointInfo {
    fn from(info: CheckpointInfo) -> Self {
        proto::CheckpointInfo {
            id: info.id.to_string(),
            name: info.name,
            created_at: Some(Timestamp {
                seconds: info.created_at as i64,
                nanos: 0,
            }),
        }
    }
}

impl TryFrom<proto::CheckpointInfo> for CheckpointInfo {
    type Error = uuid::Error;

    fn try_from(proto: proto::CheckpointInfo) -> Result<Self, Self::Error> {
        Ok(CheckpointInfo {
            id: Uuid::parse_str(&proto.id)?,
            name: proto.name,
            created_at: proto.created_at.map(|t| t.seconds as u64).unwrap_or(0),
        })
    }
}

fn catalog_timestamp_to_proto(value: chrono::DateTime<chrono::Utc>) -> Timestamp {
    Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

fn catalog_timestamp_from_proto(
    value: Option<Timestamp>,
    field: &str,
) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    let value = value.ok_or_else(|| anyhow::anyhow!("missing {field}"))?;
    let nanos = u32::try_from(value.nanos)
        .map_err(|_| anyhow::anyhow!("{field} has negative nanoseconds"))?;
    chrono::DateTime::from_timestamp(value.seconds, nanos)
        .ok_or_else(|| anyhow::anyhow!("{field} is outside the supported timestamp range"))
}

impl From<CustomerCatalogRecord> for proto::BranchInfo {
    fn from(record: CustomerCatalogRecord) -> Self {
        debug_assert_eq!(record.kind, CustomerResourceKind::Branch);
        Self {
            volume_id: record.volume_id.to_string(),
            id: record.resource_id.to_string(),
            name: record.name,
            state: record.state,
            parent_id: record.parent_id.map(|id| id.to_string()),
            origin_checkpoint_id: record.origin_checkpoint_id.map(|id| id.to_string()),
            observed_generation: record.observed_generation,
            created_at: Some(catalog_timestamp_to_proto(record.created_at)),
            updated_at: Some(catalog_timestamp_to_proto(record.updated_at)),
            deleted_at: record.deleted_at.map(catalog_timestamp_to_proto),
            customer_metadata_json: Value::Object(record.customer_metadata).to_string(),
        }
    }
}

impl TryFrom<proto::BranchInfo> for CustomerCatalogRecord {
    type Error = anyhow::Error;

    fn try_from(record: proto::BranchInfo) -> Result<Self, Self::Error> {
        let metadata = serde_json::from_str::<Value>(&record.customer_metadata_json)
            .map_err(|error| anyhow::anyhow!("invalid customer metadata JSON: {error}"))?;
        let Value::Object(customer_metadata) = metadata else {
            anyhow::bail!("customer metadata JSON is not an object");
        };
        Ok(Self {
            volume_id: Uuid::parse_str(&record.volume_id)?,
            resource_id: Uuid::parse_str(&record.id)?,
            kind: CustomerResourceKind::Branch,
            name: record.name,
            state: record.state,
            parent_id: record
                .parent_id
                .map(|id| Uuid::parse_str(&id))
                .transpose()?,
            origin_checkpoint_id: record
                .origin_checkpoint_id
                .map(|id| Uuid::parse_str(&id))
                .transpose()?,
            observed_generation: record.observed_generation,
            created_at: catalog_timestamp_from_proto(record.created_at, "created_at")?,
            updated_at: catalog_timestamp_from_proto(record.updated_at, "updated_at")?,
            deleted_at: record
                .deleted_at
                .map(|timestamp| catalog_timestamp_from_proto(Some(timestamp), "deleted_at"))
                .transpose()?,
            customer_metadata,
        })
    }
}

impl From<FileAccessEvent> for proto::FileAccessEvent {
    fn from(event: FileAccessEvent) -> Self {
        let (operation, params) = match event.operation {
            FileOperation::Read { offset, length } => (
                proto::FileOperation::Read as i32,
                proto::OperationParams {
                    offset: Some(offset),
                    length: Some(length),
                    ..Default::default()
                },
            ),
            FileOperation::Write { offset, length } => (
                proto::FileOperation::Write as i32,
                proto::OperationParams {
                    offset: Some(offset),
                    length: Some(length),
                    ..Default::default()
                },
            ),
            FileOperation::Create { mode } => (
                proto::FileOperation::Create as i32,
                proto::OperationParams {
                    mode: Some(mode),
                    ..Default::default()
                },
            ),
            FileOperation::Remove => (
                proto::FileOperation::Remove as i32,
                proto::OperationParams::default(),
            ),
            FileOperation::Rename { new_path } => (
                proto::FileOperation::Rename as i32,
                proto::OperationParams {
                    new_path: Some(new_path),
                    ..Default::default()
                },
            ),
            FileOperation::Mkdir { mode } => (
                proto::FileOperation::Mkdir as i32,
                proto::OperationParams {
                    mode: Some(mode),
                    ..Default::default()
                },
            ),
            FileOperation::Readdir { count } => (
                proto::FileOperation::Readdir as i32,
                proto::OperationParams {
                    length: Some(count as u64),
                    ..Default::default()
                },
            ),
            FileOperation::Lookup { filename } => (
                proto::FileOperation::Lookup as i32,
                proto::OperationParams {
                    filename: Some(filename),
                    ..Default::default()
                },
            ),
            FileOperation::Setattr { mode } => (
                proto::FileOperation::Setattr as i32,
                proto::OperationParams {
                    mode,
                    ..Default::default()
                },
            ),
            FileOperation::Link { new_path } => (
                proto::FileOperation::Link as i32,
                proto::OperationParams {
                    new_path: Some(new_path),
                    ..Default::default()
                },
            ),
            FileOperation::Symlink { target } => (
                proto::FileOperation::Symlink as i32,
                proto::OperationParams {
                    link_target: Some(target),
                    ..Default::default()
                },
            ),
            FileOperation::Mknod { mode } => (
                proto::FileOperation::Mknod as i32,
                proto::OperationParams {
                    mode: Some(mode),
                    ..Default::default()
                },
            ),
            FileOperation::Trim { offset, length } => (
                proto::FileOperation::Trim as i32,
                proto::OperationParams {
                    offset: Some(offset),
                    length: Some(length),
                    ..Default::default()
                },
            ),
            FileOperation::Fallocate {
                offset,
                length,
                mode,
            } => (
                proto::FileOperation::Fallocate as i32,
                proto::OperationParams {
                    offset: Some(offset),
                    length: Some(length),
                    mode: Some(mode),
                    ..Default::default()
                },
            ),
            FileOperation::Fsync => (
                proto::FileOperation::Fsync as i32,
                proto::OperationParams::default(),
            ),
        };

        proto::FileAccessEvent {
            timestamp: Some(Timestamp {
                seconds: event.timestamp as i64,
                nanos: 0,
            }),
            operation,
            path: event.path,
            params: Some(params),
        }
    }
}

impl From<ObjectAccessEvent> for proto::ObjectAccessEvent {
    fn from(event: ObjectAccessEvent) -> Self {
        let (operation, params) = match event.operation {
            ObjectOperation::Get { offset, length } => (
                proto::ObjectOperation::ObjGet as i32,
                proto::ObjectParams {
                    offset,
                    length,
                    ..Default::default()
                },
            ),
            ObjectOperation::Head => (
                proto::ObjectOperation::ObjHead as i32,
                proto::ObjectParams::default(),
            ),
            ObjectOperation::Put { size } => (
                proto::ObjectOperation::ObjPut as i32,
                proto::ObjectParams {
                    size: Some(size),
                    ..Default::default()
                },
            ),
            ObjectOperation::PutMultipart => (
                proto::ObjectOperation::ObjPutMultipart as i32,
                proto::ObjectParams::default(),
            ),
            ObjectOperation::Delete => (
                proto::ObjectOperation::ObjDelete as i32,
                proto::ObjectParams::default(),
            ),
            ObjectOperation::List => (
                proto::ObjectOperation::ObjList as i32,
                proto::ObjectParams::default(),
            ),
            ObjectOperation::Copy { to } => (
                proto::ObjectOperation::ObjCopy as i32,
                proto::ObjectParams {
                    target_path: Some(to),
                    ..Default::default()
                },
            ),
            ObjectOperation::Rename { to } => (
                proto::ObjectOperation::ObjRename as i32,
                proto::ObjectParams {
                    target_path: Some(to),
                    ..Default::default()
                },
            ),
        };

        proto::ObjectAccessEvent {
            timestamp: Some(Timestamp {
                seconds: event.timestamp as i64,
                nanos: 0,
            }),
            store: event.store.to_string(),
            operation,
            path: event.path,
            params: Some(params),
            duration_us: event.duration_us,
            error: event.error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proto_file_operation_display_is_named_and_padded() {
        use proto::FileOperation as P;
        let cases = [
            (P::Read, "read   "),
            (P::Write, "write  "),
            (P::Create, "create "),
            (P::Remove, "remove "),
            (P::Rename, "rename "),
            (P::Mkdir, "mkdir  "),
            (P::Readdir, "readdir"),
            (P::Lookup, "lookup "),
            (P::Setattr, "setattr"),
            (P::Link, "link   "),
            (P::Symlink, "symlink"),
            (P::Mknod, "mknod  "),
            (P::Trim, "trim   "),
            (P::Fsync, "fsync  "),
        ];
        for (op, want) in cases {
            assert_eq!(format!("{op}"), want);
            assert_eq!(want.len(), 7, "display is padded to a fixed width");
        }
    }

    #[test]
    fn checkpoint_info_round_trips() {
        let id = Uuid::parse_str("12345678-1234-1234-1234-1234567890ab").unwrap();
        let info = CheckpointInfo {
            id,
            name: "snap".to_string(),
            created_at: 1_700_000_000,
        };
        let p = proto::CheckpointInfo::from(info.clone());
        assert_eq!(p.id, id.to_string());
        assert_eq!(p.name, "snap");
        assert_eq!(p.created_at.unwrap().seconds, 1_700_000_000);

        let back = CheckpointInfo::try_from(p).unwrap();
        assert_eq!(back.id, id);
        assert_eq!(back.name, "snap");
        assert_eq!(back.created_at, 1_700_000_000);
    }

    #[test]
    fn checkpoint_info_try_from_rejects_a_bad_uuid() {
        let p = proto::CheckpointInfo {
            id: "not-a-uuid".to_string(),
            name: "x".to_string(),
            created_at: None,
        };
        assert!(CheckpointInfo::try_from(p).is_err());
    }

    #[test]
    fn checkpoint_info_try_from_defaults_a_missing_timestamp() {
        let p = proto::CheckpointInfo {
            id: Uuid::nil().to_string(),
            name: "x".to_string(),
            created_at: None,
        };
        assert_eq!(CheckpointInfo::try_from(p).unwrap().created_at, 0);
    }

    #[test]
    fn customer_branch_info_round_trips_without_storage_fields() {
        let now = zerofs::catalog::catalog_timestamp(chrono::Utc::now());
        let mut metadata = serde_json::Map::new();
        metadata.insert("project".to_string(), Value::String("alpha".to_string()));
        let record = CustomerCatalogRecord {
            volume_id: Uuid::new_v4(),
            resource_id: Uuid::new_v4(),
            kind: CustomerResourceKind::Branch,
            name: "main".to_string(),
            state: "ready".to_string(),
            parent_id: Some(Uuid::new_v4()),
            origin_checkpoint_id: Some(Uuid::new_v4()),
            observed_generation: 7,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            customer_metadata: metadata,
        };

        let proto = proto::BranchInfo::from(record.clone());
        assert!(!proto.customer_metadata_json.contains("root"));
        assert_eq!(CustomerCatalogRecord::try_from(proto).unwrap(), record);
    }

    #[test]
    fn customer_branch_info_rejects_non_object_metadata() {
        let now = catalog_timestamp_to_proto(chrono::Utc::now());
        let proto = proto::BranchInfo {
            volume_id: Uuid::new_v4().to_string(),
            id: Uuid::new_v4().to_string(),
            name: "main".to_string(),
            state: "ready".to_string(),
            parent_id: None,
            origin_checkpoint_id: None,
            observed_generation: 1,
            created_at: Some(now),
            updated_at: Some(now),
            deleted_at: None,
            customer_metadata_json: "[]".to_string(),
        };
        assert!(CustomerCatalogRecord::try_from(proto).is_err());
    }

    fn proto_event(operation: FileOperation) -> proto::FileAccessEvent {
        proto::FileAccessEvent::from(FileAccessEvent {
            timestamp: 42,
            operation,
            path: "/p".to_string(),
        })
    }

    fn code(operation: FileOperation) -> i32 {
        proto_event(operation).operation
    }

    fn params(operation: FileOperation) -> proto::OperationParams {
        proto_event(operation).params.unwrap()
    }

    #[test]
    fn every_operation_maps_to_its_proto_code() {
        use proto::FileOperation as P;
        assert_eq!(
            code(FileOperation::Read {
                offset: 0,
                length: 0
            }),
            P::Read as i32
        );
        assert_eq!(
            code(FileOperation::Write {
                offset: 0,
                length: 0
            }),
            P::Write as i32
        );
        assert_eq!(code(FileOperation::Create { mode: 0 }), P::Create as i32);
        assert_eq!(code(FileOperation::Remove), P::Remove as i32);
        assert_eq!(
            code(FileOperation::Rename {
                new_path: "x".into()
            }),
            P::Rename as i32
        );
        assert_eq!(code(FileOperation::Mkdir { mode: 0 }), P::Mkdir as i32);
        assert_eq!(code(FileOperation::Readdir { count: 0 }), P::Readdir as i32);
        assert_eq!(
            code(FileOperation::Lookup {
                filename: "x".into()
            }),
            P::Lookup as i32
        );
        assert_eq!(
            code(FileOperation::Setattr { mode: None }),
            P::Setattr as i32
        );
        assert_eq!(
            code(FileOperation::Link {
                new_path: "x".into()
            }),
            P::Link as i32
        );
        assert_eq!(
            code(FileOperation::Symlink { target: "x".into() }),
            P::Symlink as i32
        );
        assert_eq!(code(FileOperation::Mknod { mode: 0 }), P::Mknod as i32);
        assert_eq!(
            code(FileOperation::Trim {
                offset: 0,
                length: 0
            }),
            P::Trim as i32
        );
        assert_eq!(code(FileOperation::Fsync), P::Fsync as i32);
        assert_eq!(
            code(FileOperation::Fallocate {
                offset: 0,
                length: 1,
                mode: 0x10,
            }),
            P::Fallocate as i32
        );
    }

    #[test]
    fn operation_params_carry_the_right_fields() {
        let p = params(FileOperation::Read {
            offset: 10,
            length: 20,
        });
        assert_eq!((p.offset, p.length), (Some(10), Some(20)));
        let p = params(FileOperation::Write {
            offset: 5,
            length: 6,
        });
        assert_eq!((p.offset, p.length), (Some(5), Some(6)));
        let p = params(FileOperation::Trim {
            offset: 1,
            length: 2,
        });
        assert_eq!((p.offset, p.length), (Some(1), Some(2)));
        let p = params(FileOperation::Fallocate {
            offset: 3,
            length: 4,
            mode: 0x11,
        });
        assert_eq!((p.offset, p.length, p.mode), (Some(3), Some(4), Some(0x11)));

        assert_eq!(
            params(FileOperation::Create { mode: 0o644 }).mode,
            Some(0o644)
        );
        assert_eq!(
            params(FileOperation::Mkdir { mode: 0o755 }).mode,
            Some(0o755)
        );
        assert_eq!(
            params(FileOperation::Mknod { mode: 0o600 }).mode,
            Some(0o600)
        );

        assert_eq!(params(FileOperation::Readdir { count: 7 }).length, Some(7));

        assert_eq!(
            params(FileOperation::Rename {
                new_path: "/n".into()
            })
            .new_path,
            Some("/n".to_string())
        );
        assert_eq!(
            params(FileOperation::Link {
                new_path: "/l".into()
            })
            .new_path,
            Some("/l".to_string())
        );
        assert_eq!(
            params(FileOperation::Lookup {
                filename: "f".into()
            })
            .filename,
            Some("f".to_string())
        );
        assert_eq!(
            params(FileOperation::Symlink {
                target: "/t".into()
            })
            .link_target,
            Some("/t".to_string())
        );

        // Setattr forwards the Option mode verbatim (Some and None).
        assert_eq!(
            params(FileOperation::Setattr { mode: Some(0o640) }).mode,
            Some(0o640)
        );
        assert_eq!(params(FileOperation::Setattr { mode: None }).mode, None);

        // Remove and Fsync carry no params.
        assert_eq!(
            params(FileOperation::Remove),
            proto::OperationParams::default()
        );
        assert_eq!(
            params(FileOperation::Fsync),
            proto::OperationParams::default()
        );
    }

    #[test]
    fn file_access_event_propagates_timestamp_and_path() {
        let e = proto_event(FileOperation::Fsync);
        assert_eq!(e.timestamp.unwrap().seconds, 42);
        assert_eq!(e.path, "/p");
    }
}
