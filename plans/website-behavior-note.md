# Website Behavior Note

This is a short future-work note, not an implementation plan.

Implementation plan for the narrower REST metadata surface:

- [plans/object-redirect-metadata-rest-plan.md](/home/justin/src/github.com/justincormack/argmin/plans/object-redirect-metadata-rest-plan.md)

## Current position

Argmin does not aim to implement S3 website behavior now.

That is still the right priority call, but this area is a real compatibility
gap and should stay visible.

## Scope to remember

"Website behavior" is more than one feature.

There are at least two distinct areas:

- object-level website redirect metadata such as
  `x-amz-website-redirect-location` / `WebsiteRedirectLocation`
- bucket website configuration APIs such as `PutBucketWebsite`,
  `GetBucketWebsite`, and `DeleteBucketWebsite`

If we revisit this later, we should treat them separately. Object metadata is a
smaller compatibility target than full website hosting behavior.

## Why this note exists

Recent AWS CLI integration checks highlighted that this is still an exposed gap.

In particular:

- `aws s3 cp --website-redirect ...` is a real client path
- we do not currently have native `s3-tests` coverage for
  `WebsiteRedirectLocation`
- broader website configuration behavior is also known to be incomplete

So even if static website hosting is out of scope for now, object-level website
redirect metadata is an AWS-visible behavior that clients can exercise.

## AWS doc gaps to remember

AWS does document parts of object redirect metadata, but the behavior is spread
across multiple API reference pages and the user guide. The gaps are mostly
about missing cross-operation semantics rather than total absence of
documentation.

Checked AWS docs:

- `PutObject`
- `CopyObject`
- `CreateMultipartUpload`
- `HeadObject`
- `GetObject`
- `POST Object`
- user guide: "How to configure website page redirects"
- `POST Policy`

The important gaps and ambiguities are:

- No single AWS page enumerates the full object redirect metadata surface.
  The user guide lists request-side support for `PUT Object`, `Initiate
  Multipart Upload`, `POST Object`, and `PUT Object - Copy`, and separately
  mentions `GET Object` returning the header. The API reference separately
  documents `HeadObject` returning
  `x-amz-website-redirect-location`. That means the full request/response
  surface has to be reconstructed across several pages.

- `HeadObject` response behavior is effectively undocumented in the user guide.
  The user guide explicitly mentions `GET Object` returning
  `x-amz-website-redirect-location`, but not `HeadObject`, even though the API
  reference does expose it there too. If we revisit this later, `HEAD` needs to
  be treated as part of the compatibility target, not inferred away.

- Multipart persistence semantics are not stated clearly.
  `CreateMultipartUpload` accepts
  `x-amz-website-redirect-location`, but the checked AWS docs do not clearly say
  that:
  - the redirect metadata is fixed at multipart initiation time
  - `CompleteMultipartUpload` has no way to add or change it
  - the completed object should surface that metadata on both `GET` and `HEAD`
  This is an important implementation detail, but it is only implied by the
  operation shapes, not described directly.

- Validation rules are inconsistent across operations.
  The `POST Object` page documents concrete constraints for
  `x-amz-website-redirect-location`: it must start with `/`, `http://`, or
  `https://`, and the value is limited to 2 KB. The checked `PutObject`,
  `CopyObject`, `CreateMultipartUpload`, and user-guide pages do not restate
  those constraints. The user guide only says that same-bucket redirects require
  a leading `/`.

- Copy semantics are only partially documented.
  `CopyObject` does document one important rule: redirect metadata is unique to
  the destination object and is not copied just because
  `x-amz-metadata-directive=COPY`; you must send
  `x-amz-website-redirect-location` explicitly. But the checked docs do not
  clearly spell out the related same-key self-copy semantics: changing website
  redirect metadata is one of the ways to make a self-copy legal even when the
  object body is unchanged.

- POST policy implications are only generic, not redirect-specific.
  `POST Object` documents the field, and the generic `POST Policy` page says
  every submitted form field must appear in the policy conditions. But the
  checked AWS docs do not call out `x-amz-website-redirect-location`
  specifically in any POST policy examples or redirect-specific guidance. If we
  implement POST support for this later, policy enforcement for that field
  should be treated as part of the feature.

- REST-endpoint versus website-endpoint behavior is documented, but only
  partially tied back to object metadata retrieval.
  The user guide says the website endpoint performs the redirect while the REST
  endpoint returns the object instead. The API reference separately says `GET`
  and `HEAD` return `x-amz-website-redirect-location`. We should keep both
  behaviors in mind: stored metadata visibility on the REST API and redirect
  serving on the website endpoint are related but distinct compatibility
  targets.

## Local test coverage gaps

Local references do not currently give us strong redirect-metadata coverage:

- `./tmp/s3-tests` currently has only a `s3website_redirect_location` pytest
  marker in `pytest.ini`; no concrete redirect-location test cases were found by
  text search.
- local Ceph source does show implementation hooks for:
  - `x-amz-website-redirect-location` on the S3 REST API response path
  - `Location` redirect behavior on the website endpoint
  - same-object copy error text that includes website redirect metadata
  But I did not find local Ceph test cases for this behavior by text search.

## If revisited later

Start small and verify each step directly against AWS:

- object upload/copy handling of `WebsiteRedirectLocation`
- `HeadObject` / `GetObject` response behavior for stored redirect metadata
- bucket website configuration CRUD APIs
- only after that, any website-endpoint-specific behavior

The important design point is not to blur these together:

- object redirect metadata compatibility is one feature
- bucket website configuration is another
- website endpoint hosting and redirect serving is a larger follow-on feature
