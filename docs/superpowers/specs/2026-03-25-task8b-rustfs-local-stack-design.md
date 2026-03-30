# Task 8b RustFS Local Stack Design

Date: 2026-03-25
Status: Draft for review

## 1. Purpose

This document defines Task 8b: replace MinIO in the local Polaris reference stack with RustFS as the default local S3-compatible backend, while making the local-stack contract provider-agnostic.

The goal is not to broaden the runtime architecture. The goal is to:

- keep the reference stack described as `Polaris + local S3-compatible object store + SQLite`
- move the local stack env and script surface to generic `OBJECT_STORE_*` naming
- make RustFS the default local backend only if it passes an explicit compatibility gate
- keep SeaweedFS out of scope unless RustFS fails that gate

## 2. Non-Goals

- no provider matrix in this task
- no production-path change away from direct cloud object storage guidance
- no changes in `iceflow-sink`, runtime, worker, or state crates to accommodate RustFS
- no parallel SeaweedFS spike
- no attempt to prove broad S3 compatibility beyond the Polaris-facing local-stack contract

## 3. Current State

Today `infra/local` still hard-codes MinIO-specific service names, env vars, bootstrap scripts, and Polaris endpoint wiring. User-facing docs have already been generalized to describe the reference stack in provider-agnostic terms, but the local stack implementation has not.

The current real-stack Polaris tests must remain unchanged. They are part of the acceptance check for the local stack, but they are not by themselves sufficient proof that a backend is compatible enough to become the default.

## 4. Design Goals

Task 8b must produce a local stack with these properties:

- provider-agnostic top-level naming in docs, env vars, compose services, and helper scripts
- RustFS as the default local object-store backend
- deterministic bucket initialization suitable for local development and CI
- explicit compatibility verification against the same endpoint, credentials, bucket, and path-style configuration that Polaris uses
- a clean fallback policy: if RustFS fails the gate, it does not become default and only then do we evaluate SeaweedFS

## 5. Local Stack Contract

The local reference stack contract is:

- one local object-store service, exposed to the rest of the stack as `object-store`
- one optional `object-store-init` helper if the backend needs separate deterministic bootstrap
- Polaris configured against the object store by generic env values and a path-style S3 endpoint
- provider-specific image names, backend env vars, and health details isolated inside `infra/local`

Public local-stack env names should become:

- `OBJECT_STORE_API_PORT`
- `OBJECT_STORE_CONSOLE_PORT`
- `OBJECT_STORE_ACCESS_KEY`
- `OBJECT_STORE_SECRET_KEY`
- `OBJECT_STORE_BUCKET`

If RustFS requires additional backend-specific env vars to start, those may remain internal to the compose service, but they must not replace the generic public contract unless they are unavoidable.

## 6. Implementation Shape

### 6.1 Compose

`infra/local/docker-compose.yml` should:

- rename the MinIO services to generic object-store service names
- switch the default backend image and startup wiring from MinIO to RustFS
- pin the RustFS image to a specific version or digest rather than `latest` so local and CI behavior stay deterministic
- feed Polaris from the generic `OBJECT_STORE_*` env surface
- keep provider-specific configuration localized to the RustFS service definition
- add an `object-store-init` helper container only if RustFS needs a separate deterministic bootstrap step

### 6.2 Bootstrap Scripts

`infra/local/object-store-init.sh` should:

- own deterministic bucket creation for the local object store
- operate against the same endpoint and credentials the stack exposes to Polaris
- be generic in name and interface, even if the first implementation uses backend-specific tooling internally
- fail loudly on bucket bootstrap errors, especially if the first implementation uses `mc` internally

`infra/local/polaris-bootstrap.sh` should:

- consume only generic object-store env names
- configure Polaris with the same bucket, endpoint, internal endpoint, and path-style setting used by the compatibility gate
- avoid any MinIO-branded naming

`infra/local/minio-init.sh` should be treated as replaced by `infra/local/object-store-init.sh` and removed from the default local stack wiring once Task 8b lands.

### 6.3 Local Env And Docs

`infra/local/.env.example`, `justfile`, `README.md`, and the implementation plan/spec docs should:

- refer to the local dependency as a local S3-compatible object store
- identify RustFS as the current default local backend
- keep production guidance centered on direct cloud object storage, especially AWS S3

## 7. RustFS Compatibility Gate

RustFS becomes the default local backend only if it passes this gate.

### 7.1 Gate Requirements

The following must all be true:

- `docker compose --env-file infra/local/.env -f infra/local/docker-compose.yml config` succeeds
- the local stack reaches healthy startup with Polaris and the RustFS-backed object store:
  - the object-store service passes its configured health check
  - `object-store-init`, if present, exits with status `0`
  - the Polaris health endpoint returns `200`
- the raw S3 compatibility probe passes against the same object-store endpoint, bucket, credentials, and path-style configuration Polaris uses
- the presigned URL audit is documented with a written finding before RustFS is treated as passing the spike
- the existing ignored real-stack Polaris smoke tests pass unchanged
- no changes are required in `iceflow-sink`, runtime, worker, or state crates

### 7.2 Raw S3 Probe Requirement

`mc` may be used for deterministic bucket initialization if it is convenient, but `mc` is not proof that RustFS is compatible enough to become default.

Promotion to default requires a raw S3 probe against the same:

- endpoint
- internal endpoint where applicable
- bucket
- credentials
- path-style setting

The probe must verify at least:

- bucket existence or deterministic bootstrap semantics
- `PutObject`
- `GetObject`
- `ListObjectsV2`
- `DeleteObject`

The pass/fail decision for RustFS must be based on these raw operations plus the unchanged Polaris smoke tests, not on `mc mb` success alone.

### 7.3 Presigned URL Audit

Task 8b must explicitly audit the current Polaris path in this repo and record whether presigned URLs are part of the local-stack contract.

That audit must inspect at least:

- `crates/iceflow-sink/src/polaris.rs`
- `infra/local/polaris-bootstrap.sh`

The audit must then produce one of two written outcomes:

- presigned URLs are part of the path under test, so the compatibility gate includes path-style presigned URL verification
- presigned URLs are not part of the current path under test, so they are explicitly excluded from the Task 8b gate with a written reason

This finding must be documented before RustFS is treated as passing the spike.

## 8. Verification

Task 8b verification should cover:

- compose config validity
- local stack startup health
- raw S3 compatibility probe against the RustFS default backend
- the documented presigned URL audit finding and whether presigned URL verification is included or explicitly excluded
- unchanged real-stack Polaris smoke tests
- updated docs and plan text that describe the stack generically while naming RustFS as the current default local backend

## 9. Failure Policy

If RustFS fails any gated criterion, Task 8b does not promote it to the default local backend. At that point, SeaweedFS becomes the next candidate to evaluate under the same contract and with the same explicit gate.

## 10. Result

The expected result of Task 8b is a local stack that is cleaner in naming, narrower in provider assumptions, and stricter about what qualifies a backend to be the default.
