use crate::db_state::SsTableId;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// L0 SSTs the writer has uploaded to the object store but not yet durably
/// published into the manifest (the pre-publish "staged" window).
///
/// Compacted-SST GC normally protects a freshly flushed L0 with a timestamp
/// watermark. This set lets an in-process GC exclude the exact SSTs the writer
/// still intends to publish, independent of that heuristic. An id is present
/// before its SST becomes visible in object storage and remains present until
/// the publishing manifest write is durable.
#[derive(Clone, Default)]
pub(crate) struct StagedSsts {
    inner: Arc<Mutex<HashSet<SsTableId>>>,
}

impl StagedSsts {
    pub(crate) fn add(&self, ids: impl IntoIterator<Item = SsTableId>) {
        let mut guard = self.inner.lock().expect("staged ssts mutex poisoned");
        guard.extend(ids);
    }

    pub(crate) fn remove(&self, ids: impl IntoIterator<Item = SsTableId>) {
        let mut guard = self.inner.lock().expect("staged ssts mutex poisoned");
        for id in ids {
            guard.remove(&id);
        }
    }

    pub(crate) fn snapshot(&self) -> HashSet<SsTableId> {
        self.inner
            .lock()
            .expect("staged ssts mutex poisoned")
            .clone()
    }
}
