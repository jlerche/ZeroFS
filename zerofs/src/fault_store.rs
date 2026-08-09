//! A reusable fault-injecting object store for tests: a pass-through decorator
//! over any inner store whose writes can be partitioned or made to fail
//! transiently, and whose reads can fail or return short bodies. Generalizes the
//! one-off wrappers in `replication::zombie` (write partition) and
//! `length_checked_object_store`'s `ShortBodyStore` (truncated reads) into one
//! programmable harness, driven through a shared [`FaultControls`] handle.

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    UploadPart,
};
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

/// Knobs shared with the test driving the store. Everything defaults to "no fault".
#[derive(Debug, Default)]
pub struct FaultControls {
    /// While true, every write (put/delete/copy) fails fast: a write partition.
    partition_writes: AtomicBool,
    /// Fail the next N `get` calls with a transient error, then resume.
    fail_next_gets: AtomicUsize,
    /// Delay the next N `get` calls before forwarding them, then resume.
    delay_next_gets: AtomicUsize,
    delay_get_millis: AtomicU64,
    /// Fail the next N `put` calls before applying them, then resume.
    fail_next_puts: AtomicUsize,
    /// Apply the next N `put` calls, then return a transient response error.
    fail_after_puts: AtomicUsize,
    /// Fail the next N `delete` calls before applying them, then resume.
    fail_next_deletes: AtomicUsize,
    /// Fail before the delete whose absolute attempt number is stored here.
    fail_delete_attempt: AtomicUsize,
    /// Apply the next N `delete` calls, then return a transient response error.
    fail_after_deletes: AtomicUsize,
    /// Return a body `truncate_bytes` short of the claimed length on the next N gets.
    truncate_next_gets: AtomicUsize,
    truncate_bytes: AtomicUsize,
    gets: AtomicUsize,
    puts: AtomicUsize,
    deletes: AtomicUsize,
    lists: AtomicUsize,
    get_bytes: AtomicU64,
    put_bytes: AtomicU64,
    listed_objects: AtomicU64,
    listed_object_bytes: AtomicU64,
    multipart_initiates: AtomicUsize,
    multipart_parts: AtomicUsize,
    multipart_completes: AtomicUsize,
    multipart_bytes: AtomicU64,
}

impl FaultControls {
    pub fn partition_writes(&self, on: bool) {
        self.partition_writes.store(on, Ordering::SeqCst);
    }
    pub fn fail_gets(&self, n: usize) {
        self.fail_next_gets.store(n, Ordering::SeqCst);
    }
    /// Delay the next `n` gets before forwarding them to the inner store.
    pub fn delay_gets(&self, n: usize, delay: std::time::Duration) {
        let millis = u64::try_from(delay.as_millis()).expect("GET delay exceeds u64 milliseconds");
        self.delay_get_millis.store(millis, Ordering::SeqCst);
        self.delay_next_gets.store(n, Ordering::SeqCst);
    }
    pub fn fail_puts(&self, n: usize) {
        self.fail_next_puts.store(n, Ordering::SeqCst);
    }
    pub fn fail_puts_after_apply(&self, n: usize) {
        self.fail_after_puts.store(n, Ordering::SeqCst);
    }
    pub fn fail_deletes(&self, n: usize) {
        self.fail_next_deletes.store(n, Ordering::SeqCst);
    }
    /// Allow `n` delete attempts, then fail the following one before apply.
    pub fn fail_delete_after_attempts(&self, n: usize) {
        let attempt = self
            .deletes
            .load(Ordering::SeqCst)
            .checked_add(n)
            .and_then(|attempt| attempt.checked_add(1))
            .expect("delete fault attempt overflow");
        self.fail_delete_attempt.store(attempt, Ordering::SeqCst);
    }
    pub fn fail_deletes_after_apply(&self, n: usize) {
        self.fail_after_deletes.store(n, Ordering::SeqCst);
    }
    /// Make the next `n` gets return a body `by` bytes short of the claimed length.
    pub fn truncate_gets(&self, n: usize, by: usize) {
        self.truncate_bytes.store(by, Ordering::SeqCst);
        self.truncate_next_gets.store(n, Ordering::SeqCst);
    }
    pub fn get_count(&self) -> usize {
        self.gets.load(Ordering::SeqCst)
    }
    pub fn put_count(&self) -> usize {
        self.puts.load(Ordering::SeqCst)
    }
    pub fn delete_count(&self) -> usize {
        self.deletes.load(Ordering::SeqCst)
    }
    pub fn list_count(&self) -> usize {
        self.lists.load(Ordering::SeqCst)
    }
    pub fn get_bytes(&self) -> u64 {
        self.get_bytes.load(Ordering::SeqCst)
    }
    pub fn put_bytes(&self) -> u64 {
        self.put_bytes.load(Ordering::SeqCst)
    }
    pub fn listed_objects(&self) -> u64 {
        self.listed_objects.load(Ordering::SeqCst)
    }
    pub fn listed_object_bytes(&self) -> u64 {
        self.listed_object_bytes.load(Ordering::SeqCst)
    }
    pub fn multipart_initiate_count(&self) -> usize {
        self.multipart_initiates.load(Ordering::SeqCst)
    }
    pub fn multipart_part_count(&self) -> usize {
        self.multipart_parts.load(Ordering::SeqCst)
    }
    pub fn multipart_complete_count(&self) -> usize {
        self.multipart_completes.load(Ordering::SeqCst)
    }
    pub fn multipart_bytes(&self) -> u64 {
        self.multipart_bytes.load(Ordering::SeqCst)
    }
    pub fn reset_counts(&self) {
        self.gets.store(0, Ordering::SeqCst);
        self.puts.store(0, Ordering::SeqCst);
        self.deletes.store(0, Ordering::SeqCst);
        self.lists.store(0, Ordering::SeqCst);
        self.get_bytes.store(0, Ordering::SeqCst);
        self.put_bytes.store(0, Ordering::SeqCst);
        self.listed_objects.store(0, Ordering::SeqCst);
        self.listed_object_bytes.store(0, Ordering::SeqCst);
        self.multipart_initiates.store(0, Ordering::SeqCst);
        self.multipart_parts.store(0, Ordering::SeqCst);
        self.multipart_completes.store(0, Ordering::SeqCst);
        self.multipart_bytes.store(0, Ordering::SeqCst);
    }
}

/// Decrement `counter` if positive; returns whether a unit was taken.
fn take_one(counter: &AtomicUsize) -> bool {
    let mut cur = counter.load(Ordering::SeqCst);
    while cur > 0 {
        match counter.compare_exchange(cur, cur - 1, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(actual) => cur = actual,
        }
    }
    false
}

#[derive(Debug)]
pub struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    ctl: Arc<FaultControls>,
}

#[derive(Debug)]
struct CountingMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    ctl: Arc<FaultControls>,
}

#[async_trait]
impl MultipartUpload for CountingMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let bytes = data.content_length() as u64;
        self.ctl.multipart_parts.fetch_add(1, Ordering::SeqCst);
        let part = self.inner.put_part(data);
        let ctl = Arc::clone(&self.ctl);
        Box::pin(async move {
            let result = part.await;
            if result.is_ok() {
                ctl.multipart_bytes.fetch_add(bytes, Ordering::SeqCst);
            }
            result
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.ctl.multipart_completes.fetch_add(1, Ordering::SeqCst);
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

impl FaultStore {
    /// Returns the store and its shared controls (default: no fault).
    pub fn new(inner: Arc<dyn ObjectStore>) -> (Arc<Self>, Arc<FaultControls>) {
        let ctl = Arc::new(FaultControls::default());
        (
            Arc::new(Self {
                inner,
                ctl: ctl.clone(),
            }),
            ctl,
        )
    }

    fn transient(op: &'static str) -> object_store::Error {
        object_store::Error::Generic {
            store: "FaultStore",
            source: format!("injected transient fault on {op}").into(),
        }
    }

    fn check_writable(&self, op: &'static str) -> object_store::Result<()> {
        if self.ctl.partition_writes.load(Ordering::SeqCst) {
            return Err(Self::transient(op));
        }
        Ok(())
    }
}

impl Display for FaultStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.ctl.puts.fetch_add(1, Ordering::SeqCst);
        let payload_bytes = payload.content_length() as u64;
        self.check_writable("put")?;
        if take_one(&self.ctl.fail_next_puts) {
            return Err(Self::transient("put"));
        }
        let result = self.inner.put_opts(location, payload, opts).await?;
        self.ctl
            .put_bytes
            .fetch_add(payload_bytes, Ordering::SeqCst);
        if take_one(&self.ctl.fail_after_puts) {
            return Err(Self::transient("put response"));
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check_writable("put_multipart")?;
        self.ctl.multipart_initiates.fetch_add(1, Ordering::SeqCst);
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(CountingMultipartUpload {
            inner,
            ctl: Arc::clone(&self.ctl),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.ctl.gets.fetch_add(1, Ordering::SeqCst);
        if take_one(&self.ctl.delay_next_gets) {
            tokio::time::sleep(std::time::Duration::from_millis(
                self.ctl.delay_get_millis.load(Ordering::SeqCst),
            ))
            .await;
        }
        if take_one(&self.ctl.fail_next_gets) {
            return Err(Self::transient("get"));
        }
        let head = options.head;
        let result = self.inner.get_opts(location, options).await?;
        if !head {
            self.ctl.get_bytes.fetch_add(
                result.range.end.saturating_sub(result.range.start) as u64,
                Ordering::SeqCst,
            );
        }
        // Head replies carry no body; only short-circuit truncation for them.
        if head || !take_one(&self.ctl.truncate_next_gets) {
            return Ok(result);
        }
        let by = self.ctl.truncate_bytes.load(Ordering::SeqCst);
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let full = result.bytes().await?;
        let short = full.slice(0..full.len().saturating_sub(by));
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(short) }).boxed()),
            meta,
            range,
            attributes,
            extensions: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = Arc::clone(&self.inner);
        let ctl = Arc::clone(&self.ctl);
        locations
            .then(move |location| {
                let inner = Arc::clone(&inner);
                let ctl = Arc::clone(&ctl);
                async move {
                    let location = location?;
                    let attempt = ctl.deletes.fetch_add(1, Ordering::SeqCst) + 1;
                    if ctl.partition_writes.load(Ordering::SeqCst)
                        || take_one(&ctl.fail_next_deletes)
                        || ctl
                            .fail_delete_attempt
                            .compare_exchange(attempt, 0, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        return Err(Self::transient("delete"));
                    }
                    inner.delete(&location).await?;
                    if take_one(&ctl.fail_after_deletes) {
                        return Err(Self::transient("delete response"));
                    }
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.ctl.lists.fetch_add(1, Ordering::SeqCst);
        let ctl = Arc::clone(&self.ctl);
        self.inner
            .list(prefix)
            .inspect(move |result| {
                if let Ok(meta) = result {
                    ctl.listed_objects.fetch_add(1, Ordering::SeqCst);
                    ctl.listed_object_bytes
                        .fetch_add(meta.size, Ordering::SeqCst);
                }
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.ctl.lists.fetch_add(1, Ordering::SeqCst);
        let result = self.inner.list_with_delimiter(prefix).await?;
        self.ctl
            .listed_objects
            .fetch_add(result.objects.len() as u64, Ordering::SeqCst);
        self.ctl.listed_object_bytes.fetch_add(
            result.objects.iter().map(|meta| meta.size).sum(),
            Ordering::SeqCst,
        );
        Ok(result)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.check_writable("copy")?;
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn fail_gets_then_recovers() {
        let (store, ctl) = FaultStore::new(Arc::new(InMemory::new()));
        let path = Path::from("k");
        store.put(&path, b"hello".to_vec().into()).await.unwrap();

        ctl.fail_gets(2);
        assert!(store.get(&path).await.is_err());
        assert!(store.get(&path).await.is_err());

        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&got[..], b"hello");
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap().len(),
            5
        );
        assert_eq!(ctl.get_count(), 4);
    }

    #[tokio::test]
    async fn delays_a_bounded_number_of_gets() {
        let (store, ctl) = FaultStore::new(Arc::new(InMemory::new()));
        let path = Path::from("k");
        store.put(&path, b"hello".to_vec().into()).await.unwrap();

        ctl.delay_gets(1, std::time::Duration::from_millis(40));
        let started = std::time::Instant::now();
        store.get(&path).await.unwrap();
        assert!(started.elapsed() >= std::time::Duration::from_millis(35));
        assert_eq!(ctl.delay_next_gets.load(Ordering::SeqCst), 0);
        store.get(&path).await.unwrap();
    }

    #[tokio::test]
    async fn partition_blocks_writes_but_not_reads() {
        let (store, ctl) = FaultStore::new(Arc::new(InMemory::new()));
        let path = Path::from("k");
        store.put(&path, b"v".to_vec().into()).await.unwrap();

        ctl.partition_writes(true);
        assert!(store.put(&path, b"v2".to_vec().into()).await.is_err());
        // Reads keep working while writes are partitioned.
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&got[..], b"v");

        ctl.partition_writes(false);
        store.put(&path, b"v3".to_vec().into()).await.unwrap();
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&got[..], b"v3");
    }

    #[tokio::test]
    async fn fail_puts_then_recovers() {
        let (store, ctl) = FaultStore::new(Arc::new(InMemory::new()));
        let path = Path::from("k");

        ctl.fail_puts(1);
        assert!(store.put(&path, b"a".to_vec().into()).await.is_err());
        store.put(&path, b"b".to_vec().into()).await.unwrap();

        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&got[..], b"b");
        assert_eq!(ctl.put_count(), 2);
    }

    #[tokio::test]
    async fn counts_lists_and_resets_operation_counts() {
        let (store, ctl) = FaultStore::new(Arc::new(InMemory::new()));
        let path = Path::from("prefix/k");
        store.put(&path, b"v".to_vec().into()).await.unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();
        let listed = store
            .list(Some(&Path::from("prefix")))
            .collect::<Vec<_>>()
            .await;
        assert_eq!(listed.len(), 1);
        assert_eq!(ctl.list_count(), 1);
        assert_eq!(ctl.get_bytes(), 1);
        assert_eq!(ctl.put_bytes(), 1);
        assert_eq!(ctl.listed_objects(), 1);
        assert_eq!(ctl.listed_object_bytes(), 1);

        let multipart_path = Path::from("multipart");
        let mut upload = store
            .put_multipart_opts(&multipart_path, PutMultipartOptions::default())
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_static(b"mp"))
            .await
            .unwrap();
        upload.complete().await.unwrap();
        assert_eq!(ctl.multipart_initiate_count(), 1);
        assert_eq!(ctl.multipart_part_count(), 1);
        assert_eq!(ctl.multipart_complete_count(), 1);
        assert_eq!(ctl.multipart_bytes(), 2);

        ctl.reset_counts();
        assert_eq!(ctl.get_count(), 0);
        assert_eq!(ctl.put_count(), 0);
        assert_eq!(ctl.delete_count(), 0);
        assert_eq!(ctl.list_count(), 0);
        assert_eq!(ctl.get_bytes(), 0);
        assert_eq!(ctl.put_bytes(), 0);
        assert_eq!(ctl.listed_objects(), 0);
        assert_eq!(ctl.listed_object_bytes(), 0);
        assert_eq!(ctl.multipart_initiate_count(), 0);
        assert_eq!(ctl.multipart_part_count(), 0);
        assert_eq!(ctl.multipart_complete_count(), 0);
        assert_eq!(ctl.multipart_bytes(), 0);
    }

    #[tokio::test]
    async fn truncates_a_bounded_number_of_gets() {
        let (store, ctl) = FaultStore::new(Arc::new(InMemory::new()));
        let path = Path::from("k");
        store.put(&path, vec![7u8; 100].into()).await.unwrap();

        ctl.truncate_gets(2, 40);
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap().len(),
            60
        );
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap().len(),
            60
        );
        // Budget exhausted: the full body comes back.
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap().len(),
            100
        );
    }

    #[tokio::test]
    async fn delete_faults_distinguish_before_and_after_apply() {
        let (store, ctl) = FaultStore::new(Arc::new(InMemory::new()));
        let path = Path::from("k");
        store.put(&path, b"v".to_vec().into()).await.unwrap();

        ctl.fail_deletes(1);
        assert!(store.delete(&path).await.is_err());
        assert!(store.head(&path).await.is_ok());

        ctl.fail_deletes_after_apply(1);
        assert!(store.delete(&path).await.is_err());
        assert!(matches!(
            store.head(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));

        store.put(&path, b"v2".to_vec().into()).await.unwrap();
        ctl.fail_delete_after_attempts(1);
        store.delete(&path).await.unwrap();
        store.put(&path, b"v3".to_vec().into()).await.unwrap();
        assert!(store.delete(&path).await.is_err());
        assert!(store.head(&path).await.is_ok());
        store.delete(&path).await.unwrap();
        assert_eq!(ctl.delete_count(), 5);
    }
}
