# Security Findings Inventory

This is the checked-in one-row-per-finding map for every file under
`security/`. The local path column is the durable local regression entry point
for the current local security suite.

## Request Parsing, Boundedness, and Transport

| Finding | Status | Local deterministic path | Notes |
| --- | --- | --- | --- |
| `security/codex-058e075` POST SigV4 auth panics on malformed UTF-8 date input | fixed | `./scripts/security-tests parser authn` | Covered by shared auth/parser hardening regressions. |
| `security/codex-14d95e1` Malformed x-amz-date can panic time-skew parsing | fixed | `./scripts/security-tests parser authn` | Covered by SigV4 date parsing regressions. |
| `security/codex-23ffb1b` Copy-source header can crash PG hashing via length assert | fixed | `./scripts/security-tests parser` | Covered by copy-source and query-validation tests. |
| `security/codex-33d8271` POST policy expiration parser can panic on invalid month | fixed | `./scripts/security-tests parser authn` | Covered by POST policy parser hardening. |
| `security/codex-6f7287e` UploadPartCopy skips multipart part-number limit validation | fixed in `fd74f68` | `./scripts/security-tests parser` | Covered by UploadPart query validation. |
| `security/codex-7ceb1e1` Delayed PutObject authorization enables extra body read | fixed | `./scripts/security-tests transport` | Covered by transport/request-handling regressions. |
| `security/codex-7db4f4b` S3 Control oracle endpoint lacks HTTPS validation | fixed | `cargo nextest run -p s3-tests --bin sts_oracle aws_oracle_endpoints_require_valid_https_urls`; `./scripts/aws-sts-oracle --endpoint http://127.0.0.1:9` must fail; `./scripts/aws-sts-oracle --s3-control-endpoint http://127.0.0.1:9` must fail | The Rust oracle parses both endpoint families before constructing signer credentials; the wrapper rejects explicit plaintext overrides before loading the dotenv or requiring AWS credentials. |
| `security/codex-7ef29a6` Batch delete skips key validation enabling authenticated DoS | stale | `./scripts/security-tests parser` | The finding is stale; `parse_delete_objects_` coverage now validates DeleteObjects keys. |
| `security/codex-fa165ff` Unbounded multipart field buffering in streaming POST causes DoS | fixed | `./scripts/security-tests parser` | Covered by multipart parsing and body-size regressions. |
| `security/codex-30ada09` Unvalidated response-* overrides can inject response headers | fixed | `./scripts/security-tests transport` | Covered by response override sanitization tests. |
| `security/codex-36005f5` Unvalidated response override headers can panic hyper responses | fixed | `./scripts/security-tests transport` | Covered by response header validation regressions. |
| `security/codex-e190b29` Duplicate Content-Length header in GetObjectPart responses | fixed | `./scripts/security-tests transport` | Covered by `s3_response_to_hyper_` regression tests. |
| `security/codex-195d31f` ListObjectVersions lacks record cap, enabling memory DoS | fixed | `./scripts/security-tests parser` | Covered by server-http and server-core bounded listing regressions. |
| `security/codex-2551b28` Delimiter listing now fetches unbounded rows, enabling DoS | fixed | `./scripts/security-tests parser` | Covered by bounded delimiter-listing regressions. |

## Authentication, Authorization, Ownership, and Bucket Policy

| Finding | Status | Local deterministic path | Notes |
| --- | --- | --- | --- |
| `security/codex-0bd370b` BlockPublicPolicy bypass via full-range SourceIp CIDR | fixed | `./scripts/security-tests authz` | Covered by bucket-policy authz regressions. |
| `security/codex-154e45d` Bucket policy denies skipped for PrincipalArn/SourceVpc clauses | fixed | `./scripts/security-tests authz` | Covered by bucket-policy differential and authz model tests. |
| `security/codex-172cf0a` PutObjectAcl bucket policy ignores ACL/grant conditions | fixed | `./scripts/security-tests authz` | Covered by authz matrix and bucket-policy ACL-condition regressions. |
| `security/codex-03b4603` Encoded copy-source versionId can bypass copy-source policy checks | invalid | `./scripts/aws-tests --test bucket_policy percent_encoded_versionid_bypasses_canonical_deny -- --nocapture` | Invalid finding; live AWS evaluates `s3:x-amz-copy-source` against the raw header spelling while CopyObject and UploadPartCopy source resolution decode `versionId`. |
| `security/codex-4fad336` UTF-8 boundary panic in bucket policy condition key lookup | fixed | `cargo nextest run -p auth key_match_prefix_rejects_non_boundary_prefix_length`; `cargo nextest run -p s3-tests --test bucket_policy test_put_bucket_policy_malformed_response_shapes` | Prefix condition-key matching now uses boundary-safe slicing; malformed non-ASCII condition keys return the AWS-pinned detailed `MalformedPolicy` response instead of panicking. |
| `security/codex-b20eff7` BinaryEquals decodes request values, weakening policy denies | invalid | `./scripts/aws-tests --test bucket_policy request_object_tag_binary_equals_deny_uses_base64_request_value -- --nocapture` | Invalid finding; live AWS applies `BinaryEquals` to base64-encoded request-context values, so raw `security=public` does not match a binary condition operand `cHVibGlj`. |
| `security/codex-52e4864` Unresolved policy variables make Not conditions match all | invalid | `./scripts/aws-tests --test bucket_policy test_bucket_policy_variables_omit_unresolved_string_operands -- --nocapture` | Invalid finding; live AWS omits unresolved string policy-variable operands, including embedded-template operands, so negative string conditions can match an empty operand list while mixed literal/unresolved arrays still evaluate only the literal operands. |
| `security/codex-6323837` Anonymous owner match grants access to anonymous uploads | resolved | `./scripts/security-tests authz` | Covered by anonymous/public-access regressions. |
| `security/codex-7acfc72` BucketOwnerEnforced does not disable public-read object ACLs | fixed | `./scripts/security-tests authz` | Covered by ownership and ACL enforcement tests. |
| `security/codex-8b367f6` AuthenticatedUsers ACLs bypass BlockPublicAcls enforcement | stale | `./scripts/security-tests authz` | The finding is stale; AuthenticatedUsers and BlockPublicAcls interactions remain covered in authz tests. |
| `security/codex-8f1ed6f` IgnorePublicAcls bypass via AuthenticatedRead object ACLs | invalid | `./scripts/security-tests authz` | Invalid finding; retained here so the existing IgnorePublicAcls coverage stays visible. |
| `security/codex-1bf5cee` Fast-path cache can briefly bypass newly added bucket policy | resolved | `./scripts/security-tests authz` | Resolved by the BOE-only read fast path plus shared generation-tracked freshness and watcher-based stale entry reload/eviction. |
| `security/codex-9e22ede` Unauthorized callers can enumerate bucket names via error codes | invalid | `./scripts/security-tests authz` | Invalid finding; discovery behavior remains covered by authz model and AWS access-matrix tests. |
| `security/codex-2ffb6d7` Unauthenticated wrong-region requests leak bucket existence | invalid | `cargo nextest run -p s3-tests --test headers test_header_sigv4_unknown_key_wrong_region_bucket_existence_precedence` | Invalid duplicate of the AWS-compatible bucket-discovery behavior; the endpoint-neutral regression pins the wrong-region existing/missing-bucket cross-product. |
| `security/codex-a25af39` Constrained same-account users still read/manage data as owners | fixed | `./scripts/security-tests authz` | Covered by ownership and constrained-user matrix tests. |
| `security/codex-bb033e7` Object lock auth order leaks bucket lock configuration | fixed | `./scripts/security-tests authz lifecycle` | Covered by local authz ordering tests and the lifecycle/object-lock group. |
| `security/codex-cece86a` RequestObjectTag policies ignored for PutObjectTagging | fixed | `./scripts/security-tests authz` | Covered by request-tag condition regressions. |
| `security/codex-1b2e520` CreateBucket can drop pending ACL metadata commands | fixed | `cargo nextest run -p storage existing_create_bucket_preserves_pending_acl_command_for_retry` | Covered by a local metadata-command convergence regression; existing CreateBucket no longer clears unrelated pending bucket commands. |
| `security/codex-6b821bc` Expiry check unintentionally moved after signature checks | fixed in `c3be106b` | `cargo nextest run -p auth expired_token`; `cargo nextest run -p auth bad_signature_rejects_signature_first` | Expired temporary credentials are rejected before signature mismatch on header, presigned, and POST SigV4 paths; static-token precedence remains separately pinned. |
| `security/codex-bb65ecc` POST uploads bypass website redirect policy condition | fixed | `cargo nextest run -p s3-tests --test bucket_policy test_bucket_policy_auth_request_context_condition_keys` | The endpoint-neutral policy-context matrix covers POST uploads locally and against AWS. |
| `security/codex-efe620a` Signature errors can echo sensitive signed headers | accepted | `cargo nextest run -p s3-tests --test sse_c test_sse_c_signature_mismatch_echoes_signed_customer_key_like_aws` | Accepted AWS-compatible behavior; the regression pins canonical-request and byte-dump echoing for a signed SSE-C key. |
| `security/codex-f2a5921` Multipart completion target lookup precedes authorization | invalid | `cargo nextest run -p s3-tests --test multipart test_complete_multipart_upload_xml_precedence test_multipart_upload_id_authorization_precedence` | Invalid duplicate of the AWS-pinned multipart XML, upload-ID existence, and authorization precedence matrix. |

## Control Plane, Raft, and Runtime Routing

| Finding | Status | Local deterministic path | Notes |
| --- | --- | --- | --- |
| `security/codex-0d61d3f` Far-future heartbeat can poison node lease deadlines | fixed in `159828be` | `cargo nextest run -p storage record_node_heartbeat_command_validates_committed_lease_deadline authority_clock_latches_forward_step_without_timestamp_ratchet authority_clock_invalidates_new_local_raft_leadership_term` | Replicated lease deadlines are bounded and recomputed, while the independent authority clock fails closed on forward discontinuities and leadership changes. |
| `security/codex-1c7eda3` Volatile Raft leases lost from durable failover fences | fixed in `41d81e90` | `cargo nextest run -p storage volatile_lease_promotion_makes_acting_set_fence_durable control_plane_raft_state_machine_rejects_stale_lease_authority_terms` | Volatile renewals are promoted into replicated state before commands consume their deadlines; promotion and authority-term checks fail closed. |
| `security/codex-491017a` Admin auth can break storage-node heartbeats | fixed in `028cb3f9` | `cargo nextest run -p argmin-s3 --bin argmin-s3 control_plane_unix_auth_verifier_includes_admin_credentials`; `cargo nextest run -p storage frontend_auth_verifier_does_not_require_storage_node_heartbeat_auth` | Admin-only auth does not implicitly require storage heartbeat credentials. |
| `security/codex-69b61e6` Authenticated control-plane handler leaves admin RPCs open | fixed | `cargo nextest run -p storage authenticated_control_plane_rejects_missing_admin_command_auth control_plane_rpc_kinds_have_explicit_auth_operations` | Every Unix control-plane RPC kind has an explicit auth class, and unsigned admin mutation requests reject before mutation. |
| `security/codex-8aee2df` Raft no-op RPCs can bypass WAL compaction | invalid; hardened in `cce2e481` | `cargo nextest run -p storage control_plane_raft_wal_backed_log_store_skips_idempotent_records` | The claimed peer-RPC WAL growth was unreachable; the log store nevertheless skips equal vote/commit, empty append, tip truncate, and repeated purge records. |
| `security/codex-908108d` Partial history can satisfy later full floor requests | fixed | `cargo nextest run -p storage cluster_map_history_pruning_preserves_only_exact_storage_node_route storage_node_refresh_filters_unrelated_history_around_exact_old_route heartbeat_persists_exact_cluster_map_history_route_references exact_old_route_retains_later_pg_introduction_boundary` | Durable exact route references and sparse reverse-delta history replace ambiguous aggregate floor retention. |
| `security/codex-a1b1bf3` Forward timestamp cap bricks control plane after downtime | fixed in `fda75348` | `cargo nextest run -p storage file_backed_authority_restarts_after_long_elapsed_downtime_without_recovery`; `cargo nextest run -p argmin-s3 --bin argmin-s3 experimental_raft_control_plane_elapsed_expiry_advances_timestamp` | A far-forward expiry that actually expires a lease may advance committed time after downtime; timestamp-only no-ops remain bounded. |
| `security/codex-cedad69` Sparse runtime maps break local historical route reconstruction | fixed in `1b900255` | `cargo nextest run -p storage runtime_map_refresh_preserves_historical_pg_routes_for_storage_cluster` | Runtime and local maps share reverse-delta reconstruction and the canonical retained-epoch set. |
| `security/codex-e41688b` Raft peer dispatcher trusts spoofable frame identity | fixed in `fa8a5fa4` | `cargo nextest run -p argmin-s3 experimental_raft_control_plane`; `cargo nextest run -p argmin-s3 --test experimental_raft_process` | Multi-node Raft peer mode requires scoped authenticated request and response envelopes before OpenRaft dispatch. |
| `security/codex-fb9957f` Frontend auth accidentally requires storage auth | fixed in `028cb3f9` | `cargo nextest run -p storage frontend_auth_verifier_does_not_require_storage_node_heartbeat_auth` | Frontend runtime-map and storage heartbeat credential requirements remain independently scoped. |

## Metadata Command Ordering and Convergence

| Finding | Status | Local deterministic path | Notes |
| --- | --- | --- | --- |
| `security/codex-2f977e8` Pending direct PUT can resurrect deleted objects | fixed in `1a4ff75` | `cargo nextest run -p storage object_delete_drains_pending_direct_put_commit_before_delete` | The finding was valid for `2f977e8`; object deletes now drain pending object metadata commands before observing or deleting current object state. |
| `security/codex-d403b97` Unguarded stream-create pending commands can be lost | fixed in `1cc2543` | `cargo nextest run -p storage pending_metadata_command_insert_rejects_existing_without_overwrite stream_put_create_drains_unrelated_pending_create_before_new_session` | Pending metadata command insertion now rejects collisions without overwrite; stream-create drains the existing pending command and releases the losing reservation before retry/error. |
| `security/codex-60b27a2` Completed-MPU prune poisons metadata digest | fixed in `022cbee`; mechanism later removed | `cargo nextest run -p storage multipart_terminal_history_has_no_standalone_table` | The original pruning fix was valid, but completed-upload rows and pruning were subsequently replaced by authenticated IDs and object-version-scoped replay state. |

## Stateful Multipart, Reclaim, Lifecycle, and Object Lock

| Finding | Status | Local deterministic path | Notes |
| --- | --- | --- | --- |
| `security/codex-1f0908f` Race in stream segment append can delete committed shards | fixed | `./scripts/security-tests stateful` | Covered by duplicate-append race regressions. |
| `security/codex-470aa8a` Streaming UploadPart reuploads leave orphaned shard data | fixed | `./scripts/security-tests stateful` | Covered by streamed part reupload/orphan cleanup tests. |
| `security/codex-b6ecc5a` Completed multipart upload tombstones accumulate indefinitely | fixed by removal | `cargo nextest run -p storage multipart_terminal_history_has_no_standalone_table` | Completed/aborted upload history is not stored. Exact completion replay is scoped to the retained object version, and abort idempotence uses authenticated upload IDs. |
| `security/codex-ecef3a4` Unbounded reclaim queue allows memory exhaustion via reads | fixed | `./scripts/security-tests stateful` | Covered by reclaim queue and payload-lease regressions. |
| `security/codex-62c20a2` Reclaim sweeper stops periodic durable scans when idle | fixed in `39554219` | `cargo nextest run -p server-core reclaim_worker_rediscovers_capacity_deferred_root_while_idle` | Each bounded idle queue poll returns to the managed worker scheduler, which continues durable discovery and reclaims capacity-deferred roots without unrelated queue activity. |
| `security/codex-a09a766` Lifecycle sweep bypasses object-lock retention checks | invalid | `./scripts/security-tests lifecycle` | Invalid finding; retained here so object-lock/lifecycle retention coverage stays visible. |
| `security/codex-cd48ea6` Object Lock headers accept past retention dates | fixed | `./scripts/security-tests lifecycle` | Covered by object-lock retention validation regressions. |

## Observability and Redaction

| Finding | Status | Local deterministic path | Notes |
| --- | --- | --- | --- |
| `security/codex-28f87fb` Control characters now allowed in keys enable log injection | fixed | `./scripts/security-tests redaction` | Covered by debug/log redaction and escaping tests. |
| `security/codex-90f9972` Tracing logs presigned URL queries and credentials | fixed | `./scripts/security-tests redaction` | Covered by redaction regressions for auth and observability surfaces. |
| `security/codex-a413960` SQLite error details now leak in S3 error responses | fixed | `./scripts/security-tests redaction` | Covered by server-http sanitization regressions. |
| `security/codex-2ce3446` DeleteBucket flight records leak raw bucket names | accepted | `./scripts/security-tests redaction` | Accepted under the observability policy: diagnostic resource names are escaped attacker-controlled text, not bearer secrets; normal request summaries still hash paths. |
| `security/codex-84bb21e` Raw storage RPC details leak into 500 diagnostics | accepted | `./scripts/security-tests redaction` | Accepted for server-side operator diagnostics when values are observability-safe escaped; secret-bearing RPC values remain prohibited. |

## Local Debug Endpoints

| Finding | Status | Local deterministic path | Notes |
| --- | --- | --- | --- |
| `security/codex-93c65d7` Unauthenticated local debug endpoints bypass S3 auth | fixed in `45497afe`, `73a36bcd` | `cargo check -p argmin-s3 --features local-debug-endpoints`; `cargo build -p argmin-s3 --release --features local-debug-endpoints` must fail | Local debug routes are compiled only for tests or the explicit feature; production default binaries reject `ARGMIN_LOCAL_DEBUG_ENDPOINT=1`, and release builds cannot include the feature. |
| `security/codex-824c430` Debug checkpoint endpoint bypasses admission controls | fixed in `45497afe`, `73a36bcd` | `cargo check -p argmin-s3 --features local-debug-endpoints`; `cargo build -p argmin-s3 --release --features local-debug-endpoints` must fail | Mutating debug checkpoint route remains a debug/test diagnostic hook only; it is unavailable in production/release artifacts. |
| `security/codex-85df181` Unauthenticated local debug leaks bucket metadata | fixed in `45497afe`, `73a36bcd` | `cargo check -p argmin-s3 --features local-debug-endpoints`; `cargo build -p argmin-s3 --release --features local-debug-endpoints` must fail | Bucket-delete debug snapshot output remains available only in explicit debug/test builds and cannot be enabled in production default or release builds. |
| `security/codex-680cc00` Debug endpoint leaks private object metadata | fixed in `45497afe`, `73a36bcd` | `cargo check -p argmin-s3 --features local-debug-endpoints`; `cargo build -p argmin-s3 --release --features local-debug-endpoints` must fail | Object-version debug samples remain available only in explicit debug/test builds and cannot be enabled in production default or release builds. |
| `security/codex-91a5cfe` Local debug endpoint leaks object keys | fixed in `45497afe`, `73a36bcd` | `cargo check -p argmin-s3 --features local-debug-endpoints`; `cargo build -p argmin-s3 --release --features local-debug-endpoints` must fail | Payload-reclaim-root key output remains available only in explicit debug/test builds and cannot be enabled in production default or release builds. |
| `security/codex-7e9d95d` Loopback debug endpoint leaks cross-bucket reclaim metadata | fixed in `45497afe`, `73a36bcd` | `cargo check -p argmin-s3 --features local-debug-endpoints`; `cargo build -p argmin-s3 --release --features local-debug-endpoints` must fail | Reclaim-claim debug output remains available only in explicit debug/test builds and cannot be enabled in production default or release builds. |

## Local Tooling

| Finding | Status | Local deterministic path | Notes |
| --- | --- | --- | --- |
| `security/codex-d9e6a71` UAT forced-overload env data directory can be deleted after passing run | fixed | `bash -n scripts/uat-forced-overload` | Informational local tooling issue; env-provided forced-overload data dirs are now treated as caller-owned like `--data-dir`. |

## Backlog Sync: Regression Path Mapping Needed

These rows restore one-row-per-finding coverage for findings whose exact local
regression command still needs to be mapped or tightened.

| Finding | Status | Local deterministic path | Notes |
| --- | --- | --- | --- |
| `security/codex-0243d93` Active PG proof floor bypass permits divergent metadata | accepted-risk | triage needed | Inventory catch-up row; verify whether a focused regression or accepted-risk rationale should be linked. |
| `security/codex-05a0abb` Streaming part race can wedge multipart metadata | fixed | triage needed | Inventory catch-up row; map the focused multipart/stream-session regression. |
| `security/codex-0659760` Restart DoS after 128 metadata commands | fixed | triage needed | Inventory catch-up row; map the focused restart/metadata command regression. |
| `security/codex-09407f8` Mismatched heartbeat epochs can poison node liveness | fixed | triage needed | Inventory catch-up row; map the focused control-plane heartbeat regression. |
| `security/codex-0c4a217` Suffix metadata transfer mutates empty destinations | fixed | triage needed | Inventory catch-up row; map the focused metadata-transfer regression. |
| `security/codex-116a5c6` Range reads now verify whole multipart parts causing DoS | fixed | triage needed | Inventory catch-up row; map the focused ranged-read regression. |
| `security/codex-15c6b67` Condvar wait can miss notifications, hanging bucket drains | invalid | triage needed | Inventory catch-up row; verify whether no regression is needed beyond the invalid disposition. |
| `security/codex-1609ae9` Partial WAL write errors leave live log unrecoverable | fixed | triage needed | Inventory catch-up row; map the focused WAL recovery regression. |
| `security/codex-1b18e25` Metadata reissue can spin on replica-only log conflicts | fixed | triage needed | Inventory catch-up row; map the focused metadata reissue regression. |
| `security/codex-1e3ab33` Repair RPC rejects valid records without CRC | fixed | triage needed | Inventory catch-up row; map the focused repair RPC regression. |
| `security/codex-22d83bb` POST Object bypasses conditional write bucket policies | invalid | triage needed | Inventory catch-up row; verify the AWS-pinned invalid disposition and local coverage. |
| `security/codex-251c254` Unchecked RPC counts allow control-plane DoS | fixed | triage needed | Inventory catch-up row; map the focused RPC count regression. |
| `security/codex-25a204e` Multipart RPC validation misses part data placement | fixed | triage needed | Inventory catch-up row; map the focused multipart RPC validation regression. |
| `security/codex-29c799b` RPC frame caps undercount bucket-delete record size | fixed | triage needed | Inventory catch-up row; map the focused RPC frame-size regression. |
| `security/codex-2b4ea5d` Raft startup checkpoint race can crash control plane | fixed | triage needed | Inventory catch-up row; map the focused raft startup regression. |
| `security/codex-2eb67fe` Stale bucket-finalizer claim can stall bucket deletion cleanup | fixed | triage needed | Inventory catch-up row; map the focused bucket-finalizer regression. |
| `security/codex-30090c8` PG proof epoch stamping can reject valid writes after epoch bump | fixed | triage needed | Inventory catch-up row; map the focused PG proof regression. |
| `security/codex-301ea7d` Restart can persist stale PG observations and brick state | fixed | triage needed | Inventory catch-up row; map the focused restart/control-plane regression. |
| `security/codex-36785d9` Unpaginated cleanup RPC can disable upload cleanup | fixed | triage needed | Inventory catch-up row; map the focused cleanup RPC regression. |
| `security/codex-43fa580` Bucket tag conditions ignored when ABAC disabled allow deny bypass | invalid | triage needed | Inventory catch-up row; verify the invalid disposition and bucket-tag condition coverage. |
| `security/codex-44ecc43` Remote write reservations use local bucket snapshots | fixed | triage needed | Inventory catch-up row; map the focused remote write-reservation regression. |
| `security/codex-477547d` ListObjectVersions delimiter can scan unbounded prefixes | fixed | triage needed | Inventory catch-up row; map the focused bounded-listing regression. |
| `security/codex-513501e` PG-scoped clear can drop unrelated pending metadata | fixed | triage needed | Inventory catch-up row; map the focused pending metadata command regression. |
| `security/codex-5395b09` Storage RPC idle timeout can drop active read handles | fixed | triage needed | Inventory catch-up row; map the focused storage RPC idle-timeout regression. |
| `security/codex-5e7f1f0` Remote frontend leaves deleted object data unreclaimed | stale | triage needed | Inventory catch-up row; verify stale disposition and reclaim coverage. |
| `security/codex-6365437` Refresh merge retains historical routes without pruning | fixed | triage needed | Inventory catch-up row; map the focused route-history pruning regression. |
| `security/codex-67175e5` POST Object conditional-policy bypass allows overwrites | invalid | triage needed | Inventory catch-up row; verify the AWS-pinned invalid disposition and POST policy coverage. |
| `security/codex-6d53602` Production Raft WAL failpoint can terminate the server | fixed | triage needed | Inventory catch-up row; map the focused production failpoint regression. |
| `security/codex-7094731` Panic possible when stripping aws-chunked Content-Encoding | fixed | triage needed | Inventory catch-up row; map the focused aws-chunked header regression. |
| `security/codex-73c6e74` HeadBucket can ignore ListBucket list-parameter Deny conditions | fixed | triage needed | Inventory catch-up row; map the focused HeadBucket/ListBucket policy regression. |
| `security/codex-73c8762` Racy metadata log allocation can wedge a PG | fixed | triage needed | Inventory catch-up row; map the focused metadata log allocation regression. |
| `security/codex-753ed86` Direct PUT cleanup bypass on pending-visibility error | fixed | triage needed | Inventory catch-up row; map the focused direct PUT cleanup regression. |
| `security/codex-78b6e3a` Metadata transfer import rejects remote peering routes | fixed | triage needed | Inventory catch-up row; map the focused metadata-transfer import regression. |
| `security/codex-791b409` Heartbeat can delete active future-epoch pending command | fixed | triage needed | Inventory catch-up row; map the focused heartbeat/pending-command regression. |
| `security/codex-79ac4af` Upgrade path can persist unparsable peering PG state | fixed | triage needed | Inventory catch-up row; map the focused upgrade/peering-state regression. |
| `security/codex-79babf8` Metadata transfer retry can persist invalid control-plane state | fixed | triage needed | Inventory catch-up row; map the focused metadata-transfer retry regression. |
| `security/codex-7b06030` RPC bulk reservation can self-throttle object reads | fixed | triage needed | Inventory catch-up row; map the focused RPC admission regression. |
| `security/codex-877cf17` Heartbeat proof validation can cause unbounded replay DoS | fixed | triage needed | Inventory catch-up row; map the focused heartbeat proof regression. |
| `security/codex-897b622` Unbounded shard repair queue can exhaust memory | fixed | triage needed | Inventory catch-up row; map the focused shard-repair queue regression. |
| `security/codex-8c99921` DeleteBucket retry can delete a recreated bucket | fixed | triage needed | Inventory catch-up row; map the focused DeleteBucket identity regression. |
| `security/codex-9123ccc` Invalid active-primary metadata proofs are silently accepted | fixed | triage needed | Inventory catch-up row; map the focused metadata proof validation regression. |
| `security/codex-918c9cb` Live metadata transfer can race in-flight writes | fixed | triage needed | Inventory catch-up row; map the focused live-transfer race regression. |
| `security/codex-91b73a8` Shard repair worker exits after first idle timeout | fixed | triage needed | Inventory catch-up row; map the focused shard-repair worker regression. |
| `security/codex-9adf249` Multipart completion ignores legacy checksum headers | invalid | triage needed | Inventory catch-up row; verify the AWS-pinned invalid disposition and checksum coverage. |
| `security/codex-9f35406` Control-plane nodes can self-issue unbounded leases | fixed | triage needed | Inventory catch-up row; map the focused lease budget regression. |
| `security/codex-a139d61` Reissue bypasses divergent metadata log check | fixed | triage needed | Inventory catch-up row; map the focused metadata reissue regression. |
| `security/codex-a22ce27` Partial version reservations can make versioned keys unwritable | fixed | triage needed | Inventory catch-up row; map the focused version reservation regression. |
| `security/codex-a23b565` Periodic shard audit can stall storage operations | fixed | triage needed | Inventory catch-up row; map the focused shard audit regression. |
| `security/codex-a8379c1` Non-reserving version IDs can race object commits | fixed | triage needed | Inventory catch-up row; map the focused version ID reservation regression. |
| `security/codex-aa6f5a6` Ignore-case policy operators only fold ASCII | fixed | triage needed | Inventory catch-up row; map the focused bucket-policy operator regression. |
| `security/codex-ac07f23` Bucket tags read before TagResource authorization | fixed | triage needed | Inventory catch-up row; map the focused TagResource authorization regression. |
| `security/codex-ad5f27b` MPU sequence drain can skip object cleanup hooks | fixed | triage needed | Inventory catch-up row; map the focused MPU sequence drain regression. |
| `security/codex-ae34427` In-flight recovery can serve stale authorization metadata | fixed | triage needed | Inventory catch-up row; map the focused recovery authorization regression. |
| `security/codex-afb03c8` Unbounded Raft WAL reread enables control-plane DoS | fixed | triage needed | Inventory catch-up row; map the focused raft WAL boundedness regression. |
| `security/codex-b2d322a` Write reservation released before writes, enabling delete races | fixed | triage needed | Inventory catch-up row; map the focused write-reservation regression. |
| `security/codex-b54587c` Durable reclaim scans can be starved by queued work | fixed | triage needed | Inventory catch-up row; map the focused durable reclaim scan regression. |
| `security/codex-b54e267` Multipart completion retry can return false NoSuchUpload | fixed | triage needed | Inventory catch-up row; map the focused multipart completion retry regression. |
| `security/codex-b6c176a` PutObject does not pin runtime map during uploads | fixed | triage needed | Inventory catch-up row; map the focused runtime-map pinning regression. |
| `security/codex-baf8625` Quoted star If-Match becomes wildcard on writes | fixed | triage needed | Inventory catch-up row; map the focused conditional-write regression. |
| `security/codex-bb3bb71` Heartbeat retry can run past requested lease budget | fixed | triage needed | Inventory catch-up row; map the focused heartbeat lease-budget regression. |
| `security/codex-bd031dc` Multipart create can leak bucket write reservations on DB errors | fixed | triage needed | Inventory catch-up row; map the focused multipart create cleanup regression. |
| `security/codex-c82e89d` Raft status scans entire WAL on every status check | fixed | triage needed | Inventory catch-up row; map the focused raft status boundedness regression. |
| `security/codex-cb7f7ea` Unbounded read-handle session state enables local DoS | fixed | triage needed | Inventory catch-up row; map the focused read-handle session regression. |
| `security/codex-d1d93d0` Raw copy-source policy checks permit encoded bypasses | invalid | triage needed | Inventory catch-up row; verify the AWS-pinned invalid disposition and copy-source policy coverage. |
| `security/codex-d2b4839` Active writes can block bucket ACL and property changes | fixed | triage needed | Inventory catch-up row; map the focused active-write blocking regression. |
| `security/codex-d4fe45c` Active PG primary can change without peering | fixed | triage needed | Inventory catch-up row; map the focused PG primary transition regression. |
| `security/codex-d8be5fb` Active PG handoff can strand the primary without a route map | fixed | triage needed | Inventory catch-up row; map the focused PG handoff regression. |
| `security/codex-e3b65ca` WAL replay bypasses static Raft peer membership validation | fixed | triage needed | Inventory catch-up row; map the focused raft membership validation regression. |
| `security/codex-e4dbc96` Unbounded metadata command cache enables memory DoS | resolved | triage needed | Inventory catch-up row; map the focused metadata command cache regression. |
| `security/codex-e77fe33` Unverified metadata digest is trusted after restart | fixed | triage needed | Inventory catch-up row; map the focused metadata digest restart regression. |
| `security/codex-ea51c5e` Stale lifecycle metadata can delete recreated-bucket objects | fixed | triage needed | Inventory catch-up row; map the focused lifecycle/recreated-bucket regression. |
| `security/codex-ed52d24` Stream session cleanup can be starved by cleanup admission | fixed | triage needed | Inventory catch-up row; map the focused stream-session cleanup regression. |
| `security/codex-f153313` Active PG proof fencing causes post-write heartbeat DoS | fixed | triage needed | Inventory catch-up row; map the focused PG proof heartbeat regression. |
| `security/codex-f504bfa` Dynamic frontend routes expire without refresh | fixed | triage needed | Inventory catch-up row; map the focused frontend route refresh regression. |
| `security/codex-fa2a075` ListParts metadata read happens before auth check | fixed | triage needed | Inventory catch-up row; map the focused ListParts authorization-order regression. |
| `security/codex-fe54c42` Shared cleanup admission can starve background cleanup | fixed | triage needed | Inventory catch-up row; map the focused cleanup admission regression. |
| `security/minimax-632837-l1` Responses do not set X-Content-Type-Options or other browser-side security headers | accepted-risk | triage needed | Inventory catch-up row; verify accepted-risk rationale and any transport coverage. |
| `security/minimax-632837-l2` ServerError::InvalidRequest reason fields are passed through to client error responses | invalid | triage needed | Inventory catch-up row; verify invalid disposition and error sanitization coverage. |
| `security/minimax-632837-m1` Streaming PUT chunked decoder has no per-chunk size cap, only outer body timeout | fixed | triage needed | Inventory catch-up row; map the focused chunked decoder boundedness regression. |
| `security/minimax-632837-m2` Aws-chunked per-chunk signature comparison uses non-constant-time equality | fixed | triage needed | Inventory catch-up row; map the focused chunk-signature comparison regression. |
| `security/minimax-632837-m3` Header-signed SigV4 requests do not validate the request timestamp against server time | fixed | triage needed | Inventory catch-up row; map the focused SigV4 date-skew regression. |
| `security/minimax-632837-m4` Data directory creation relies on umask for permissions rather than an explicit chmod | fixed | triage needed | Inventory catch-up row; map the focused data-directory permissions regression. |
| `security/minimax-632837-m5` Storage-node socket directory validation has a TOCTOU window and does not check setuid/setgid/sticky bits | fixed | triage needed | Inventory catch-up row; map the focused socket-directory validation regression. |

## Integrity, Checksums, and Low-Level Implementation

| Finding | Status | Local deterministic path | Notes |
| --- | --- | --- | --- |
| `security/codex-2cf85f1` AArch64 PMULL CRC path enabled without PMULL feature check | invalid | `./scripts/security-tests integrity` | Invalid finding; retained here so the low-level integrity surface is still tracked. |
| `security/codex-32d164e` AArch64 CRC fast path lacks PMULL feature gating | invalid | `./scripts/security-tests integrity` | Invalid finding; retained here so the low-level integrity surface is still tracked. |
| `security/codex-6e5f441` POST object ignores SSE-C headers, storing data unencrypted | invalid | `./scripts/security-tests transport integrity` | Invalid finding; SSE-C transport behavior remains covered under local transport tests and AWS oracle suites. |
| `security/codex-ae442f5` PCLMUL CRC loop uses out-of-bounds pointer arithmetic | fixed | `./scripts/security-tests integrity` | Covered by checksum crate regressions. |
| `security/codex-f6004f1` Streaming PUTs without content hash bypass payload integrity | fixed | `./scripts/security-tests parser integrity` | Covered by streaming checksum and parser hardening regressions. |
