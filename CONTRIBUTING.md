<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Contributing

For local changes, add or update tests and run the local verification gate:

```bash
./scripts/ci
```

For changes affecting AWS-facing behavior, also add or update shared tests
and run the affected cases against both Argmin and AWS. The AWS entry point is:

```bash
./scripts/aws-tests
```

See the [testing guide](guides/testing.md) for prerequisites, AWS account and
credential setup, and selecting individual suites.
