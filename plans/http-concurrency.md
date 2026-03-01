
# HTTP Server and Concurrency Design

## Problem

We are having issues related to the http library we chose to use to get started. We made the right choice to start simple, but especially with CopyObject and the end to end tests we are running into all sorts of issues with it being slow, and there are also conformance issues around some UTF8 edge cases. So it is time to move on and explore options to replace this library.

While we do so, and potentially influencing the choice of replacement library, there are some things we need to bear in mind.

We want to support concurrency effectively. Our essential unit of parallelism is the PG. Each PG can read and write from a set of disks, and we can have multiple PGs on the same disks. We can get enough operations to drive good SSD performance just from multiple PGs, eg if we have 64 PGs on a disk that is fine, we don't need each to queue a lot of operations, even a single item per PG would probably be sufficient, and we can increase number of PGs if we need more.

That is the back end so to speak, but at the front end, receiving http requests, we should be able to accept these on any node, but even then, we need to be able to handle a lot of outstanding requests, as there are more PGs than nodes or disks. However we do not want to accept an unbounded number of requests - we need to be able to apply backpressure to requests and not use unbounded memory. So we should have a fixed allocation of buffers not allocate dynamically. This might suggest using an io_uring type setup.

## Options considered

### io_uring-native runtimes (glommio, monoio, compio)

These offer native io_uring for both network and disk I/O, which aligns with the
fixed-buffer and backpressure goals. However:

- **HTTP layers are immature.** Glommio has no built-in HTTP server; its hyper compat
  layer requires `unsafe impl Send` shims. Monoio's `monoio-http` is explicitly WIP.
  Compio has `cyper` (hyper bridge) but it's young and untested at scale.
- **All types are `!Send`** (thread-per-core model). This makes integrating with
  existing HTTP libraries that assume `Send` difficult.
- **tokio-uring is dormant.** No meaningful releases since 2022.
- **Safety concerns are real.** All io_uring runtimes share fundamental issues with
  Rust's ownership model — kernel operations continue after futures are dropped,
  TCP connections leak on cancellation. Manageable but requires care.
- **Apache Iggy's migration (Feb 2026)** from tokio to compio is the most relevant
  case study (see https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/).
  They achieved 92% reduction in P9999 tail latency at 32 partitions, but it was a
  **complete rewrite**, not incremental. They ended up needing "shared-something"
  hybrid patterns rather than pure shared-nothing. Claims here are based on their
  published blog post; we have not independently verified.

**Verdict:** Not ready for an HTTP frontend today. The ecosystem needs another year.
Compio is the most promising and worth watching.

### hyper + Tower (on tokio)

- **Excellent HTTP compliance.** Handles chunked encoding, range requests, streaming
  bodies, HTTP/1.1 and HTTP/2. Used by Garage (https://garagehq.deuxfleurs.fr/) and
  RustFS (https://github.com/rustfs/rustfs), both S3-compatible storage systems.
- **Strong backpressure via Tower middleware.** `ConcurrencyLimitLayer` (semaphore-based),
  `LoadShed`, `RequestBodyLimitLayer`. Tower's `Service::poll_ready` propagates
  backpressure through the stack.
- **Streaming body model** — bodies are `Stream<Item = Bytes>` chunks, not fully
  buffered by the framework.
- **No native io_uring.** But hyper 1.0's IO traits were designed for forward-compat
  with completion-based I/O (see https://github.com/hyperium/hyper/issues/2140).
  Expert consensus (from Rust forum discussions, not our own benchmarks) is that
  io_uring shows limited benefit for socket I/O — the win is in disk I/O.
- **Proven at scale** in many production systems.

### axum (on hyper + Tower)

Thin ergonomic layer on hyper. Adds routing extractors and middleware composition.
Used by RustFS. For an S3 server we need custom routing anyway (path-style vs
virtual-host-style), so axum adds convenience but not necessity. Worth considering
for cleaner handler signatures.

### Custom io_uring HTTP server

Maximum control over buffers and I/O batching (the tarweb approach: pre-allocated
fixed buffer per connection, zero syscall with kTLS). But means writing or integrating
an HTTP parser ourselves and handling all edge cases. Enormous compliance burden for
S3. Only makes sense if HTTP frontend becomes the bottleneck, which is unlikely before
disk I/O does.

## Recommendation: split architecture

The key insight is that **io_uring's benefits for networking are debated, but for disk
I/O they are clear**. Our workload has both:

- **Network I/O (HTTP frontend):** epoll/tokio is good enough. io_uring adds
  complexity without clear wins for socket I/O.
- **Disk I/O (PG shard reads/writes):** io_uring batching across 64 PGs per disk
  is where the real win is. Fixed buffers eliminate kernel-userspace copies.

This suggests a **split architecture**:

### Phase 1: Replace tiny_http with hyper (now)

Replace tiny_http with hyper, using Tower for concurrency control. This immediately
fixes the known problems (slow under concurrency, UTF-8 conformance) and gives us
a production-quality HTTP layer.

**Migration scope.** The existing code has a good internal abstraction: `S3Request`
and `S3Response` are plain Rust structs with no tiny_http dependency. The multipart
parser works on `&[u8]`. The router is pure logic. The core changes are:

1. `main.rs` — server creation and request loop
2. `http/mod.rs:handle_request()` — change the request type parameter
3. `http/mod.rs:send_response()` — adapt to hyper's response builder
4. `http/request.rs:S3Request::from_http()` — read from hyper's Request type

However, the move to async will have ripple effects beyond these 4 locations:
request body handling changes (hyper bodies are async streams, not sync Read),
response construction may need to change for streaming, error mapping needs to
produce hyper-compatible responses, shutdown/graceful-drain behavior needs design,
and the coordinator call boundary changes from sync to async dispatch via channels.
The S3 business logic itself stays the same but the glue around it is more than
4 touch points.

**PG dispatch model: mailbox with worker pool.**

A thread per PG is the conceptual model — each PG serializes its own work and owns
its SQLite connection and shard files. However, with many PGs (e.g. 256 PGs across
4 disks) a literal thread per PG may cause excessive context-switch overhead. The
implementation should use a **PG mailbox + fixed worker pool**: each PG has a bounded
channel (its mailbox), and a pool of worker threads drains the mailboxes. A worker
picks up a PG's mailbox, processes one or more requests, then moves on. This
preserves per-PG serialization (only one worker processes a given PG's mailbox at a
time) while bounding the number of OS threads.

The async HTTP frontend sends work to PG mailboxes. PG worker threads are fully
synchronous — they loop over their assigned mailbox, process requests, and send
responses back via a oneshot channel.

**Backpressure and resource budgets:**

Backpressure operates at multiple levels with hard caps:

- **Max open connections.** Configured at the hyper server level. Prevents
  connection exhaustion. Excess connections get TCP RST.
- **Max concurrent requests.** Global `ConcurrencyLimitLayer` caps total in-flight
  requests across all PGs. This bounds aggregate memory since each in-flight request
  holds at most one request body (up to 256 MB) and one response body. The product
  `max_concurrent_requests * max_body_size` is the worst-case memory envelope.
  This must be tuned to available RAM.
- **Per-PG mailbox depth.** Bounded channel per PG (e.g. depth 2-4). When full, the
  HTTP layer returns an overload response immediately rather than queueing unboundedly.
- **Per-request body limit.** `RequestBodyLimitLayer` enforces 256 MB per request.
  This is a per-request cap, not an aggregate cap — the aggregate is bounded by
  `max_concurrent_requests` above.
- **Queue wait timeout.** If a request sits in a PG mailbox for longer than a
  configured timeout (e.g. 30s), it should be failed rather than processed stale.
  The client has likely already given up.

**Overload responses must be S3-compatible:**

- When the global concurrency limit is hit: return `503 ServiceUnavailable` with
  S3 error code `SlowDown` and a `Retry-After` header. This tells well-behaved
  clients to back off with exponential retry.
- When a PG mailbox is full: return `503 ServiceUnavailable` with `SlowDown`.
  The client doesn't need to know about PGs.
- When queue wait timeout expires: return `503 ServiceUnavailable` with
  `ServiceUnavailable` error code.
- Body: standard S3 XML error format (`<Error><Code>SlowDown</Code>...</Error>`).

**Async boundary:** The HTTP layer is async (tokio + hyper). PG workers are
synchronous. The boundary is the PG mailbox channel. The HTTP handler awaits the
oneshot response channel after sending work to the PG mailbox. This is clean: async
code never touches SQLite or shard files directly.

### Phase 2: io_uring for disk I/O (when disk becomes the bottleneck)

Add io_uring for shard reads/writes using the `io-uring` crate directly or via
compio's disaggregated driver. Keep HTTP on tokio/hyper. The PG abstraction is a
natural boundary — each PG's disk operations can be submitted as an io_uring batch.

This is where fixed buffers matter: pre-allocate shard-sized buffers per PG,
submit batched reads/writes without kernel-userspace copies.

### Phase 3: full io_uring (future, if needed)

If the HTTP frontend becomes the bottleneck, evaluate migrating to compio + cyper.
By then the ecosystem should be more mature. Hyper 1.0's IO traits were designed
with this forward-compatibility in mind.

## Open questions

- **hyper directly vs axum?** Axum adds ergonomics but another abstraction layer.
  For S3 routing we need custom logic either way. Garage uses hyper directly;
  RustFS uses axum. Leaning toward hyper directly for fewer layers and easier
  control over streaming.
- **Worker pool sizing?** The pool needs enough threads to keep all disks busy but
  not so many that context-switching dominates. A reasonable starting point is
  threads = number of disks * some multiplier (e.g. 2-4x), capped at CPU count.
  Needs benchmarking.
- **Streaming for large objects?** Currently everything is fully buffered (256 MB
  cap). Hyper supports streaming bodies natively, and we will want to stream
  request bodies through EC encoding eventually to reduce memory pressure. But
  this is a separate piece of work that requires reworking the EC pipeline — we
  should do the hyper migration first with the existing fully-buffered model,
  then tackle streaming as a follow-up.
- **Connection: close vs keep-alive?** The integration tests revealed issues with
  hyper's connection pool and tiny_http's keep-alive handling. With hyper on both
  sides this should be cleaner, but worth validating.
