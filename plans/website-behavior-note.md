# Website Behavior Note

This is a short future-work note, not an implementation plan.

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
