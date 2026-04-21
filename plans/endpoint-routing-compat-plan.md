# Endpoint Routing Compatibility Plan

## Summary

Argmin currently exposes one S3 API endpoint and largely treats requests as
path-style requests on that endpoint.

That is sufficient for the current AWS-compatibility surface, but it leaves a
coherent deferred area:

- explicit `Host` / endpoint-shape validation
- virtual-hosted-style bucket addressing
- website endpoints
- `s3-control` endpoint routing
- other bucket-in-host routing variants

This plan separates that work from the auth / bucket-ABAC plans. The ABAC
behavior itself is now pinned; what remains is the broader endpoint-routing
surface needed to match AWS host and URL forms.

## Current State

Implemented today:

- normal S3 REST API behavior on the current endpoint
- path-style bucket/object routing
- minimal `TagResource` / `UntagResource` support for bucket ABAC

Known compatibility gaps:

- `TagResource` / `UntagResource` are still accepted on the ordinary S3
  endpoint instead of requiring the AWS `s3-control` host shape
- virtual-hosted-style bucket addressing is not implemented
- website endpoint behavior is not implemented
- host-sensitive endpoint families are not yet distinguished locally

Related references:

- [guides/aws-compatibility.md](/home/justin/src/github.com/justincormack/argmin/guides/aws-compatibility.md)
- [plans/aws-auth-compat-plan.md](/home/justin/src/github.com/justincormack/argmin/plans/aws-auth-compat-plan.md)
- [plans/website-behavior-note.md](/home/justin/src/github.com/justincormack/argmin/plans/website-behavior-note.md)

## Scope

In scope:

- request routing that depends on the request host or URL form
- validating AWS-specific host shapes where the API requires them
- mapping bucket-in-host versus path-style semantics
- the minimal local architecture needed to support more than one logical
  endpoint shape

Out of scope for the first pass:

- broader `s3-control` feature parity beyond the already-implemented bucket
  ABAC subset
- full website hosting semantics beyond identifying the endpoint shape and
  request routing model
- access points, multi-region access points, and other large AWS endpoint
  families unless they become necessary to structure the host-routing layer

## Work Items

### 1. Define the local endpoint model

Decide how Argmin should represent multiple AWS-facing endpoint surfaces:

- normal S3 REST endpoint
- website endpoint
- `s3-control` endpoint

Likely outputs:

- a small endpoint-kind enum or equivalent request classification
- explicit host parsing and normalization rules
- tests that make the chosen routing rules unambiguous

### 2. Pin `s3-control` routing shape

Lock down the exact routing rules for the currently implemented control-plane
subset:

- `https://{account_id}.s3-control.{region}.amazonaws.com`
- required `Host` shape
- any required `x-amz-account-id` validation already pinned at the API level

Then tighten local behavior so the bucket-ABAC `TagResource` / `UntagResource`
subset is no longer accepted on the ordinary S3 endpoint.

### 3. Pin virtual-hosted-style bucket routing

Map AWS behavior for:

- `bucket.s3.{region}.amazonaws.com`
- path-style versus host-style precedence
- request canonicalization and bucket-name extraction
- error shape on malformed or mismatched host/bucket combinations

This should be done before adding more host-sensitive endpoint families, since
it defines the base bucket-in-host model.

### 4. Separate website endpoint routing from website behavior

Treat website endpoints as two layers:

- endpoint-shape detection / routing
- website-serving semantics

The first part belongs here. The broader website-serving behavior can continue
to live under the website note / follow-up work.

### 5. Revisit host validation and allowlisting

Once the endpoint kinds exist, decide:

- whether local requests should reject unknown host shapes strictly
- how bucket-name-in-host validation interacts with future multi-host or
  distributed deployment work

This should be driven by AWS-pinned request behavior rather than by ad hoc
allowlists.

## Suggested Order

1. Minimal endpoint-kind model in the HTTP layer
2. `s3-control` host enforcement for the already-implemented ABAC subset
3. virtual-hosted-style bucket routing
4. website endpoint routing
5. broader host validation cleanup

## Open Questions

1. Should host-sensitive routing be introduced as a pure HTTP-layer concern, or
   should endpoint kind become part of the coordinator request model?

2. How much of the future multi-host deployment shape should be anticipated now,
   versus keeping this plan narrowly AWS-routing-focused?

3. Should website endpoint routing stay fully separate from website-serving
   semantics, or should the first minimal website endpoint pass include a very
   small serving behavior slice?
