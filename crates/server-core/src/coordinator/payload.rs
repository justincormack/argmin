use super::*;

#[derive(Debug)]
pub(super) struct PayloadBufferPool {
    default_capacity: usize,
    max_cached: usize,
    cached: Mutex<Vec<Vec<u8>>>,
    #[cfg(test)]
    allocations: std::sync::atomic::AtomicUsize,
}

pub(super) struct PooledPayloadBuffer {
    pool: Arc<PayloadBufferPool>,
    buf: Option<Vec<u8>>,
}

#[derive(Debug)]
pub(super) struct SharedPayloadBuffer {
    pool: Option<Arc<PayloadBufferPool>>,
    buf: Vec<u8>,
}

pub(super) struct EncodeScratchPool {
    scratch_len: usize,
    max_cached: usize,
    cached: Mutex<Vec<Vec<u8>>>,
    #[cfg(test)]
    allocations: std::sync::atomic::AtomicUsize,
}

pub(super) struct EncodeScratch<'a> {
    pool: &'a EncodeScratchPool,
    buf: Option<Vec<u8>>,
}

impl ReadChunk {
    pub(super) fn from_vec(data: Vec<u8>) -> Self {
        let len = data.len();
        Self {
            data: Arc::new(SharedPayloadBuffer::from_unpooled(data)),
            start: 0,
            end: len,
        }
    }

    pub(super) fn from_shared_range(
        data: Arc<SharedPayloadBuffer>,
        start: usize,
        end: usize,
    ) -> Self {
        debug_assert!(start <= end);
        debug_assert!(end <= data.len());
        Self { data, start, end }
    }

    pub fn len(&self) -> usize {
        self.end - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl AsRef<[u8]> for ReadChunk {
    fn as_ref(&self) -> &[u8] {
        &self.data.buf[self.start..self.end]
    }
}

impl std::ops::Deref for ReadChunk {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl PayloadBufferPool {
    pub(super) fn new(ec_config: EcConfig) -> Arc<Self> {
        let default_capacity = segment_payload_buffer_capacity(ec_config);
        let max_cached = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(1);
        Arc::new(Self {
            default_capacity,
            max_cached,
            cached: Mutex::new(Vec::new()),
            #[cfg(test)]
            allocations: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    pub(super) fn checkout(self: &Arc<Self>, required_capacity: usize) -> PooledPayloadBuffer {
        let min_capacity = required_capacity.max(self.default_capacity);
        let mut cached = lock_mutex_unpoisoned(&self.cached);
        let maybe_idx = cached
            .iter()
            .rposition(|buf| buf.capacity() >= min_capacity);
        let mut buf = maybe_idx.map_or_else(
            || {
                #[cfg(test)]
                self.allocations.fetch_add(1, Ordering::Relaxed);
                Vec::with_capacity(min_capacity)
            },
            |idx| cached.swap_remove(idx),
        );
        drop(cached);
        buf.clear();
        PooledPayloadBuffer {
            pool: Arc::clone(self),
            buf: Some(buf),
        }
    }

    fn recycle(&self, mut buf: Vec<u8>) {
        if buf.capacity() < self.default_capacity {
            return;
        }
        buf.clear();
        let mut cached = lock_mutex_unpoisoned(&self.cached);
        if cached.len() < self.max_cached {
            cached.push(buf);
        }
    }

    #[cfg(test)]
    pub(super) fn allocation_count(&self) -> usize {
        self.allocations.load(Ordering::Relaxed)
    }
}

impl PooledPayloadBuffer {
    pub(super) fn resize_zeroed(&mut self, len: usize) {
        self.buf.as_mut().unwrap().resize(len, 0);
    }

    pub(super) fn truncate(&mut self, len: usize) {
        self.buf.as_mut().unwrap().truncate(len);
    }

    pub(super) fn into_shared(mut self) -> Arc<SharedPayloadBuffer> {
        Arc::new(SharedPayloadBuffer {
            pool: Some(Arc::clone(&self.pool)),
            buf: self.buf.take().unwrap(),
        })
    }
}

impl std::ops::Deref for PooledPayloadBuffer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        self.buf.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledPayloadBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf.as_mut().unwrap()
    }
}

impl Drop for PooledPayloadBuffer {
    fn drop(&mut self) {
        let Some(buf) = self.buf.take() else {
            return;
        };
        self.pool.recycle(buf);
    }
}

impl SharedPayloadBuffer {
    pub(super) fn from_unpooled(buf: Vec<u8>) -> Self {
        Self { pool: None, buf }
    }

    pub(super) fn len(&self) -> usize {
        self.buf.len()
    }
}

impl std::ops::Deref for SharedPayloadBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

impl Drop for SharedPayloadBuffer {
    fn drop(&mut self) {
        let Some(pool) = self.pool.take() else {
            return;
        };
        pool.recycle(std::mem::take(&mut self.buf));
    }
}

impl EncodeScratchPool {
    pub(super) fn new(ec_config: EcConfig) -> Self {
        let scratch_len = encode_parity_scratch_len(ec_config);
        let max_cached = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(1);
        Self {
            scratch_len,
            max_cached,
            cached: Mutex::new(Vec::new()),
            #[cfg(test)]
            allocations: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(super) fn checkout(&self) -> EncodeScratch<'_> {
        let buf = lock_mutex_unpoisoned(&self.cached)
            .pop()
            .unwrap_or_else(|| {
                #[cfg(test)]
                self.allocations.fetch_add(1, Ordering::Relaxed);
                vec![0u8; self.scratch_len]
            });
        EncodeScratch {
            pool: self,
            buf: Some(buf),
        }
    }

    #[cfg(test)]
    pub(super) fn allocation_count(&self) -> usize {
        self.allocations.load(Ordering::Relaxed)
    }
}

impl EncodeScratch<'_> {
    pub(super) fn as_mut_slice(&mut self, len: usize) -> &mut [u8] {
        debug_assert!(len <= self.pool.scratch_len);
        &mut self.buf.as_mut().unwrap()[..len]
    }

    pub(super) fn as_slice(&self, len: usize) -> &[u8] {
        debug_assert!(len <= self.pool.scratch_len);
        &self.buf.as_ref().unwrap()[..len]
    }
}

impl Drop for EncodeScratch<'_> {
    fn drop(&mut self) {
        let Some(buf) = self.buf.take() else {
            return;
        };
        let mut cached = lock_mutex_unpoisoned(&self.pool.cached);
        if cached.len() < self.pool.max_cached {
            cached.push(buf);
        }
    }
}

pub(super) fn encode_parity_scratch_len(ec_config: EcConfig) -> usize {
    let k = ec_config.data_shards as usize;
    let m = ec_config.parity_shards as usize;
    let padded = max_stored_segment_size().div_ceil(k) * k;
    let shard_size = padded / k;
    shard_size.saturating_mul(m)
}

pub(super) fn segment_payload_buffer_capacity(ec_config: EcConfig) -> usize {
    let k = ec_config.data_shards as usize;
    max_stored_segment_size().div_ceil(k) * k
}

pub(super) fn max_stored_segment_size() -> usize {
    // Encrypted objects store an authentication tag alongside the largest
    // logical segment.
    INTERNAL_SEGMENT_SIZE.saturating_add(SSE_C_SEGMENT_TAG_LEN)
}

impl SegmentPayloadRecord {
    pub(super) fn stored_size(&self) -> usize {
        self.size as usize + self.encryption.segment_ciphertext_extra_len()
    }
}

impl Drop for PayloadLease {
    fn drop(&mut self) {
        if self.storage_node().release_object_payload_lease(
            &self.bucket,
            &self.key,
            self.generation_id,
        ) == 0
        {
            match self.runtime.object_payload_reclaim_exists_for(
                &self.bucket,
                &self.key,
                self.generation_id,
            ) {
                Ok(true) | Err(_) => self.runtime.enqueue_object_payload_reclaim_for(
                    &self.bucket,
                    &self.key,
                    self.generation_id,
                ),
                Ok(false) => {}
            }
        }
    }
}

impl PayloadLease {
    fn storage_node(&self) -> &SharedStorageNode {
        &self.runtime.storage_node
    }
}
