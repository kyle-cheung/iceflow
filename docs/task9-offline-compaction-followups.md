# Task 9 Offline Compaction Follow-Ups

These items were left intentionally outside the core Task 9 implementation because they do not block the offline compaction feature itself, but they should be cleaned up later.

## Deferred Verification Cleanup

- `just test-compact` is the intended compact verification entrypoint in `justfile`, but the current harness ships a `just` shim that only allows `test-fast`. Re-run `just test-compact` in a normal developer shell or CI environment with a real `just` binary.
- `cargo test -p greytl-cli --test compact` uses `crates/greytl-cli/tests/support/polaris_mock.rs`, which binds `127.0.0.1`. In restricted sandboxes, this suite may require an unsandboxed run even when the code is healthy.

## Deferred Polaris Auth Follow-Up

- `crates/greytl-cli/src/commands/compact_catalog.rs` currently uses an unauthenticated `reqwest::blocking::Client` for Polaris preflight.
- This is consistent with the current compact CLI contract and mirrors a broader CLI-level gap rather than a Task 9-specific regression, but it means `compact` preflight will fail against Polaris deployments that require auth.
- When the CLI contract is widened, add bearer-token and/or client-credentials support to `PolarisCompactionCatalog` using the same auth shape already available on `greytl-sink::PolarisSink`.

## Deferred Lint Cleanup

- `cargo clippy -p greytl-cli -- -D warnings` is still blocked by pre-existing `clippy::too_many_arguments` findings in `crates/greytl-types/src/mutation.rs`.
- This lint debt predates Task 9 and should be addressed as a separate cleanup change if branch-wide clippy cleanliness is required.
