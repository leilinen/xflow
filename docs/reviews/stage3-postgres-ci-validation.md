# Stage 3 PostgreSQL CI Validation

## Scope

This report records the PostgreSQL 16 validation for the Share to Bot stage 3
persistence implementation. It covers Tweet UPSERT semantics, batch Backfill,
Telegram favorites, transaction rollback, concurrency, schema initialization,
and performance trends. No production database, X API, Telegram delivery, or
deployment is involved.

## Implementation under test

- Tweet inserts use the `tweets(tweet_id)` unique constraint with
  `INSERT ... ON CONFLICT DO NOTHING RETURNING tweet_id`.
- Rows not returned by the insert are refreshed with one set-based `UPDATE`.
- A single Tweet does not open an explicit transaction; a new Tweet uses one
  DML statement and an existing Tweet uses two.
- Backfill input is deduplicated by Tweet ID, retains the last payload, sorts
  IDs to keep a consistent unique-index lock order, and writes batches of 500.
- Each Backfill batch uses one short transaction. No advisory lock is used.
- Favorites remain idempotent through their `(chat_id, tweet_id)` primary key.
- The favorites pagination index covers
  `(chat_id, created_at DESC, tweet_id DESC)`.

## Test isolation

CI uses the PostgreSQL 16 service container configured in
`.github/workflows/ci.yml` and the dedicated `xflow_test` database. Every
integration or performance test creates a unique schema, sets `search_path` on
every pool connection, and drops the schema after the test. `TEST_DATABASE_URL`
is required; there is no fallback to an application database.

## Local validation

- `cargo fmt --check --all`: passed.
- `cargo clippy --all-targets --all-features -- -D warnings`: passed.
- `cargo test --lib`: passed, 107 tests.
- PostgreSQL tests were intentionally not run locally because no isolated local
  PostgreSQL test database is available.

## GitHub Actions evidence

- Branch: `stage3-postgres-ci-validation`
- Validated commit: `1f69b913f8cd7eea3a2774eb2e11d3b001b84ffb`
- Run: [GitHub Actions run 44665490799](https://github.com/leilinen/xflow/actions/runs/44665490799)
- `fmt / clippy / unit tests`: passed in 31 seconds. This included
  `cargo fmt --check --all`,
  `cargo clippy --all-targets --all-features -- -D warnings`, and
  `cargo test --lib` (107 tests).
- `PostgreSQL 16 integration tests`: passed in 1 minute 49 seconds. The full
  `rust_service` integration target executed 41 tests against the PostgreSQL
  service, including concurrent same/different-ID UPSERTs, concurrent
  Tweet-plus-Favorite persistence, rollback, foreign keys and cascade,
  keyset pagination, Backfill counts and duplicate input, repeated schema
  initialization, and bounded deadlock/timeout checks.
- `PostgreSQL 16 performance trend`: passed in 1 minute 26 seconds. Its one
  performance test completed in 2.18 seconds and emitted the measurements
  below.
- The Docker publishing job was skipped as intended because this was not the
  `main` branch.
- Performance artifact:
  [stage3-postgres-performance](https://github.com/leilinen/xflow/actions/runs/44665490799/artifacts/14597672587),
  artifact ID `14597672587`, SHA-256
  `a823cabd1246d96bfcfc4a9f9ae95278c07ef887ca505ab560ff02e2f3e8e107`.

The PostgreSQL service reported healthy before tests ran. `TEST_DATABASE_URL`
was masked in logs and pointed at the dedicated CI database. The CI test role
successfully created and dropped per-test schemas, which verifies the required
schema privileges without involving production credentials.

## PostgreSQL performance results

The following values were emitted by PostgreSQL 16 on GitHub's hosted runner.
Times and throughput are rounded for readability; the artifact retains the
full-precision values.

| Tweets | Scenario | Batch time | Throughput | Batch round trips | Previous sequential round trips | Post-run pool acquire wait |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 100 | all new | 2.569 ms | 38,922 tweets/s | 3 | 100 | 0.152 ms |
| 100 | all repeated | 2.375 ms | 42,111 tweets/s | 4 | 100 | 0.165 ms |
| 1,000 | all new | 17.466 ms | 57,256 tweets/s | 6 | 1,000 | 0.152 ms |
| 1,000 | all repeated | 18.273 ms | 54,724 tweets/s | 8 | 1,000 | 0.202 ms |

With a batch size of 500, the measured implementation reduced the expected
protocol round trips from 100/1,000 individual statements to 3--8 transaction
and DML round trips. The sub-millisecond post-run acquisition measurements do
not indicate connection-pool waiting in this run. These numbers are trend
evidence rather than a fixed performance threshold.

The removed advisory-lock implementation and previous sequential writer are not
executed for benchmarking. Running thousands of individual auto-commits made
the hosted-runner test a storage durability benchmark rather than a useful CI
trend. Round-trip counts are instead derived directly from the executed query
shape: batch figures include `BEGIN`, application DML statements, and `COMMIT`;
the previous path required one UPSERT statement per Tweet. Current batch time,
throughput, and post-run pool acquisition are measured for all four required
scenarios. The isolated performance connection uses `synchronous_commit=off`;
integration tests continue to use PostgreSQL's default durability setting.

## Known limitations

- GitHub-hosted runner timings are trend evidence, not stable latency SLOs.
- The single-row two-statement conflict path can report an error if another
  writer deletes the Tweet between conflict detection and UPDATE. The service
  has no production Tweet deletion path; database-level cascade behavior is
  nevertheless tested separately.
- The old advisory-lock implementation was deliberately not restored solely
  to benchmark it. Therefore the performance evidence compares measured batch
  behavior and query-shape round trips, not wall-clock timings from the removed
  implementation.

## Acceptance conclusion

Stage 3 meets the requested acceptance criteria on the validated commit:
formatting, Clippy, unit tests, PostgreSQL integration tests, concurrency and
transaction behavior, Backfill accounting, and the PostgreSQL performance
trend all passed. No Telegram delivery path was modified. The remaining
limitations above are non-blocking for proceeding to the next stage, but this
report does not authorize or implement stage 4.
