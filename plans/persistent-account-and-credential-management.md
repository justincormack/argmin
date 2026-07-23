# Persistent Account And Credential Management

## Scope

This plan will cover the transition from the current process-local static
credential store to a durable account and credential management model suitable
for real multi-account deployments.

This is intentionally a new plan, separate from the completed ownership
foundations work and the existing auth compatibility follow-ups. The goal here
is not request signing correctness, ACL semantics, or bucket policy
evaluation. The goal is to define how accounts and credentials exist, persist,
and are administered in the product.

In scope:
- durable account records
- durable access-key / secret-key credential records
- account identity fields needed by the rest of the system
- bootstrap and administrative flows for creating and managing accounts
- server startup changes needed to stop treating credentials as static process
  config

Out of scope:
- SigV4 protocol changes
- bucket policy evaluation
- IAM policy language
- STS or federation APIs

The initial STS issuance work is tracked separately in
[sts-assume-role-issuance.md](sts-assume-role-issuance.md). That plan should
establish provider and identity seams that this durable plan can later
implement without coupling persistence to the HTTP or SigV4 layers.

## Why This Needs Its Own Plan

Today production authentication is still backed by a single configured access
key and secret key loaded into an in-memory `CredentialStore` at startup. That
is enough for local development and the current compatibility work, but it is
not a real account system.

The recent ownership work means the storage and authorization layers now
understand durable account identity much better than the production credential
source does. We should close that gap deliberately rather than accreting
ad-hoc user management onto the existing startup path.

This plan should define the durable account substrate that later policy, ACL,
and administrative work can rely on.

## Current State

Today the repository has three different practical modes:

1. Local integration tests
- use a temporary storage directory
- populate an in-memory credential store with fixed test credentials

2. External compatibility testing
- use credentials supplied by the environment or `.env`
- rely on the external system to provide real account persistence

3. Production server binary
- loads one access key pair from environment variables
- inserts that key into an in-memory credential store at startup
- provides no persistent account registry and no runtime account-management
  surface

That means the server currently supports multi-account behavior in the core
authorization model, but not in the production credential-management model.

## Target Outcome

At the end of this plan, account and credential management should be a normal
durable part of the system rather than test harness setup or process-local
configuration.

At a minimum, the finished design needs to answer:
- where account records live
- where access keys live
- how canonical IDs and display names are sourced and updated
- how the first administrative account is bootstrapped
- how later accounts and credentials are created, rotated, disabled, and
  deleted
- how server workers read and cache account / credential state safely

## Status

Intro only for now. The implementation phases, schema shape, APIs, and test
strategy should be filled in once we decide the intended operational model.
Concrete account modelling should also be defined here when implementation
starts, including the durable representation of accounts, their identity
fields, and how those records relate to credential records.

When this work is implemented, re-check AWS conformance for principal-specific
`AccessDenied` responses. Current compatibility work only matches the generic
anonymous/private-object denial shape; AWS also emits caller-specific denial
messages that include the requester principal plus the denied action/resource,
and those are expected to depend on richer persistent account identity.
