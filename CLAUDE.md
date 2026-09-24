# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Spring Batch demo: "Daily E-commerce Order & Invoice Processing". Ingests a CSV of order line items, validates and aggregates them into invoices, persists results to PostgreSQL, writes a CSV summary, and demonstrates a real skip-and-log error-handling path (not a happy-path-only toy).

Stack: Spring Boot 4.1.1 (Java 21), Spring Batch (bundled Spring Batch 6.0.5), Spring Data JPA / Hibernate, Spring Web, Spring Boot Actuator + Micrometer (Prometheus registry), Lombok, PostgreSQL, Prometheus, Grafana.

## Commands

```bash
# Start Postgres (batchdemo/batchdemo/batchdemo on localhost:5432)
docker compose up -d

# Run the app (Windows: use mvnw.cmd)
./mvnw.cmd spring-boot:run

# Compile only
./mvnw.cmd compile

# Run all tests
./mvnw.cmd test

# Run a single test class
./mvnw.cmd test -Dtest=InvoiceCalculatorTest

# Run a single test method
./mvnw.cmd test -Dtest=DefaultOrderLineValidatorTest#rejectsANegativeQuantity

# Regenerate src/main/resources/data/order-line-items.csv (100k rows: most
# orders get 1 line item, some 2-3; 14 rows deliberately invalid to exercise
# the skip-and-log path - see generate.py)
python generate.py
```

There is no separate lint step; Lombok annotation processing runs as part of `compile`/`testCompile` in the `maven-compiler-plugin` execution in `pom.xml`.

`DemoApplicationTests` (`@SpringBootTest`) loads the full application context and therefore requires Postgres to be running (`docker compose up -d` first) — it will fail otherwise. `DefaultOrderLineValidatorTest` and `InvoiceCalculatorTest` are plain unit tests with no Spring context / DB dependency.

### Triggering the job manually

```bash
curl -X POST http://localhost:8080/api/batch/jobs/order-processing
curl http://localhost:8080/api/batch/jobs/executions/{id}

# Operational control (JobOperator - see Architecture below)
curl -X POST http://localhost:8080/api/batch/jobs/executions/{id}/stop
curl -X POST http://localhost:8080/api/batch/jobs/executions/{id}/restart
curl -X POST http://localhost:8080/api/batch/jobs/executions/{id}/abandon
```

`spring.batch.job.enabled=false` is set, so the job does **not** run automatically on startup — it must be triggered via the REST endpoint above.

### Observability

```bash
curl http://localhost:8080/actuator/health              # includes a `db` component
curl http://localhost:8080/actuator/metrics/spring.batch.job
curl http://localhost:8080/actuator/metrics/spring.batch.step
curl http://localhost:8080/actuator/prometheus           # spring_batch_job_seconds_*, spring_batch_step_seconds_*
```

`docker compose up -d` also starts:
- **Grafana** (`http://localhost:3000`, anonymous viewer access — no login needed), with two provisioned dashboards:
  - "Batch Job & Step Executions" — every `JobExecution`/`StepExecution` row, queried directly from Postgres (not Prometheus — see Architecture below for why that distinction matters).
  - "Batch Metrics (Prometheus)" — job/step duration trends + JVM heap, queried from Prometheus.
- **Prometheus** (`http://localhost:9090`) — actually scrapes `/actuator/prometheus` every 15s (the endpoint alone, from the Actuator addition, was inert until this existed). Since the app runs on the Windows host, not in `docker-compose`, the scrape target in `prometheus/prometheus.yml` is `host.docker.internal:8080`, not `localhost:8080` — confirmed reachable via Docker Desktop's built-in host DNS. Check `http://localhost:9090/targets` if metrics ever look stale.

## Architecture

### Spring Batch 6.0 package relocations (important, non-obvious)

This project's Spring Batch version (6.0.5, bundled with Boot 4.1.1) relocated most classes from their pre-6.0 packages. Get these wrong and the build fails with "package does not exist" even though the dependency is on the classpath:

- `org.springframework.batch.item.*` → `org.springframework.batch.infrastructure.item.*` (`ItemReader`, `ItemWriter`, `ItemProcessor`, `Chunk`, `ExecutionContext`, `FlatFileItemReader`/`Writer`, `FieldSet`, `FieldSetMapper`, `FieldExtractor`, `JpaItemWriter`, `CompositeItemWriter`, `AbstractItemStreamItemReader`, etc.)
- `org.springframework.batch.core.*` split into subpackages: `Job`/`JobInstance` → `core.job`, `JobExecution` → `core.job`, `JobParameters`/`JobParametersBuilder` → `core.job.parameters`, `Step`/`StepExecution` → `core.step`, `JobExecutionListener`/`SkipListener` → `core.listener`, `JobExplorer` → `core.repository.explore`, `JobExecutionAlreadyRunningException`/`JobRestartException`/`JobInstanceAlreadyCompleteException` → `core.launch`, `JobParametersInvalidException` was renamed to `InvalidJobParametersException` and moved to `core.job.parameters`.
- `JobBuilder`, `StepBuilder`, `JobRepository`, `JobLauncher`, `@StepScope` kept their pre-6.0 package locations.

### JobRepository must be explicitly JDBC-backed

Boot 4.1's batch auto-configuration (`BatchAutoConfiguration`) applies only `@EnableBatchProcessing` with no `@EnableJdbcJobRepository`, so **by default Spring Batch uses an in-memory `JobRepository`** even with a `DataSource` present — `spring.batch.jdbc.initialize-schema` no longer exists as a property in this version (Boot's `BatchProperties` only has a `job` sub-property now).

To get real, persistent job/step execution history in Postgres, `BatchJobConfig` explicitly declares both `@EnableBatchProcessing` and `@EnableJdbcJobRepository`. This works because Boot's own `BatchAutoConfiguration` backs off (`@ConditionalOnMissingBean(annotation = EnableBatchProcessing.class)`) once any other `@EnableBatchProcessing`-annotated class is found — declaring `@EnableJdbcJobRepository` alone, without also declaring `@EnableBatchProcessing` on the same class, is silently ignored. The `BATCH_*` schema itself is created via Boot's generic SQL initializer, pointed at Spring Batch's bundled DDL: `spring.sql.init.schema-locations=classpath:org/springframework/batch/core/schema-postgresql.sql` in `application.properties`. `spring.sql.init.continue-on-error=true` is set because that DDL isn't idempotent (no `IF NOT EXISTS`), so repeated app restarts against the same DB would otherwise fail on the second boot.

### Batch job/step metrics come from Spring Batch itself, not app code

Spring Batch 6 ships built-in Micrometer `Observation` instrumentation for every job and step execution (`org.springframework.batch.core.observability.*`), wired in automatically by `@EnableBatchProcessing` — already present via `BatchJobConfig`. It's inert until something provides an `ObservationRegistry` bean; Boot 4.1's `BatchObservationAutoConfiguration` (confirmed via `javap`: `@ConditionalOnBean(ObservationRegistry.class)`) is what turns it on, and `spring-boot-starter-actuator` is what creates that registry. So adding Actuator + `micrometer-registry-prometheus` lit up job/step timing metrics with **zero code changes** to the batch pipeline. Confirmed metric names (via the class file constant pools, not docs): `spring.batch.job` (tags: `spring.batch.job.name`, `spring.batch.job.status`) and `spring.batch.step` (tags: `spring.batch.step.name`, `spring.batch.step.job.name`, `spring.batch.step.status`, `spring.batch.step.type`) — both exported as Prometheus histograms (`spring_batch_job_seconds_*`, `spring_batch_step_seconds_*`) plus an `_active_seconds` gauge for in-flight executions. `management.endpoints.web.exposure.include` in `application.properties` controls which Actuator endpoints are exposed — currently `health,info,metrics,prometheus`, unauthenticated (fine for a local demo, would need locking down before anything internet-facing).

### Grafana visualizes execution history via a Postgres datasource, not Prometheus

Prometheus only holds aggregated counters/timers — it can tell you "`buildInvoicesStep` has run 3 times, total 2 seconds," never "here is execution #11, which ran at 13:24 and completed in 0.55s." Row-level execution history already exists in `BATCH_JOB_EXECUTION`/`BATCH_STEP_EXECUTION` (written by the JDBC job repository, see above), so `grafana/provisioning/datasources/postgres.yml` points Grafana directly at Postgres via a SQL datasource (`uid: batchdemo-postgres`) instead. `grafana/dashboards/batch-executions.json` (auto-loaded via `grafana/provisioning/dashboards/dashboards.yml`) queries those tables directly with raw SQL table panels — no new metrics, no code changes, same "existing data, new way to look at it" pattern as the Actuator addition. The Step Executions panel's `Rollbacks` column is a nice side effect: it's the direct fingerprint of the retry policy (`buildInvoicesStep` rows always show `rollback_count=1`, one simulated failure + recovery per run) and of the skip policy in step 1 (`ingestLineItemsStep` rows show `rollback_count` from the chunk-scan-and-skip recovery, alongside `Skips=6`).

### The Prometheus trend dashboard, and a Micrometer gotcha worth knowing

`grafana/dashboards/batch-metrics-prometheus.json` is the complementary dashboard — trends over time from actual Prometheus data (`spring_batch_job_seconds_*`, `spring_batch_step_seconds_*`, JVM heap), via the `Prometheus` datasource (`uid: batchdemo-prometheus`, `grafana/provisioning/datasources/prometheus.yml`) pointed at the Prometheus container. Its "Max Job Duration" stat panel queries `spring_batch_job_seconds_max` — be aware that Micrometer's `Timer` `max` is a *rolling-window* max (decays back toward 0 if no new observation lands within the step interval), not a lifetime max, so it can legitimately read `0` shortly after the last job run rather than staying pinned at the slowest execution ever seen. The `_sum`/`_count` fields used elsewhere on the dashboard are true, never-reset cumulative totals and don't have this quirk — prefer them over `_max` for anything that needs to stay meaningful between runs.

### `@StepScope` readers must be typed by their concrete class, not the `ItemReader` interface

A `@Bean @StepScope` method that returns the `ItemReader<T>` interface type causes the framework to never invoke `open()`/`close()` on it (Spring Batch logs a warning about this: "If using @StepScope on a @Bean method, be sure to return the implementing class"). Since `DistinctOrderIdItemReader` (see below) holds iteration state that's only populated in `open()`, getting this wrong makes the step silently process zero items with no error. Always declare the `@Bean` method's return type as the concrete class (see `BuildInvoicesStepConfig.distinctOrderIdItemReader`).

### Job design: two steps, staging table in between

`orderProcessingJob` = `ingestLineItemsStep` → `buildInvoicesStep` → `mergeInvoiceSummaryStep` (the last one just recombines partitioned output — see "Both steps run partitioned" below). A single-step "aggregate consecutive CSV rows into one order" reader was deliberately avoided because it would require assuming the CSV is pre-sorted by `orderId` and would complicate per-item skip semantics.

- **`ingestLineItemsStep`** (`batch/step1/`, wired in `config/IngestLineItemsStepConfig`): reads the CSV row-by-row into `OrderLineCsvRecord`, validates each row independently, and writes valid ones into the flat `OrderLineItemStaging` entity (no FK relations — just a landing table with a `processed` flag). This is where all per-row error handling happens, since Spring Batch's skip API operates at the single-item level:
  - Malformed lines (bad column count, non-numeric price, bad date) → `FlatFileParseException` → skip on read.
  - Business-rule violations (negative qty/price, blank customer name, blank orderId) → `InvalidOrderLineException` → skip on process.
  - Both registered via `.faultTolerant().skip(...).skipLimit(...)`; a `RejectedRecordSkipListener` logs the raw record + reason to `rejected-rows.csv` via the `RejectedRecordSink` abstraction (`CsvRejectedRecordSink`).
- **`buildInvoicesStep`** (`batch/step2/`, wired in `config/BuildInvoicesStepConfig`): `DistinctOrderIdItemReader` reads distinct unprocessed `orderId`s from staging; `InvoiceAggregationProcessor` loads all staged lines for an id, builds the real `Order`/`OrderLineItem` entities, and delegates tax/total math to `InvoiceCalculator`. The `CompositeItemWriter` fans each result out to three single-purpose delegates: `OrderPersistenceItemWriter` (JPA save, cascades to line items + invoice), the `FlatFileItemWriter` for `invoice-summary.csv`, and `StagingMarkProcessedItemWriter` (flips `processed=true` in the same chunk transaction).

### Retry is simulated on purpose, the same way skip uses deliberately-bad CSV rows

`buildInvoicesStep` also demonstrates `.faultTolerant().retry(...)` — a genuinely transient exception (a dropped DB connection, a lock timeout) would never actually fire against a healthy local Postgres, so `FlakyOrderPersistenceSimulator` (`batch/step2/`) throws `TransientInvoiceWriteException` exactly once per job run, from the top of `OrderPersistenceItemWriter.write(...)`, before anything in the chunk touches the DB. The chunk transaction rolls back cleanly (nothing partially written), Spring Batch retries the same chunk — re-running `InvoiceAggregationProcessor.process(...)` for its buffered items too, which is safe because that processor already re-checks `existsByOrderNumber` — and the retry succeeds because the simulator's flag is now tripped. `perRunStateResetListener` (in `BatchJobConfig`, the same `beforeJob` listener that resets the rejects file) re-arms it every run via `flakySimulator.reset()`, so every run shows exactly one retry, deterministically. `InvoiceWriteRetryListener` logs a `WARN` on each retry attempt so it's visible in the console. Toggle off with `batch.simulate-transient-write-failures=false`; retry count via `batch.retry-limit` (default 3).

`TransientInvoiceWriteException` is *also* registered as skippable (`.skip(TransientInvoiceWriteException.class).skipLimit(properties.getSkipLimit())`, reusing the same `batch.skip-limit` step 1 uses), so the two policies compose the way Spring Batch intends: retry first, and only if `retryLimit` attempts are all exhausted does Spring Batch scan the chunk item-by-item and skip the one offending item instead of failing the whole step/job. In the current deterministic simulation this path never actually triggers (the simulator always succeeds by the 2nd attempt), but it's there so a *genuinely* persistent transient failure degrades gracefully instead of failing the job outright. Note: unlike step 1, no `SkipListener` is registered here, so a skip on this path is currently silent (no audit trail) — `readCount`/`writeCount`/`skipCount` on the `buildInvoicesStep` `StepExecution` would still reflect it, just nothing gets written to a file.

### Both steps run partitioned, not single-threaded

`ingestLineItemsStep` and `buildInvoicesStep` are each a manager `Step` built via `.partitioner(workerStepName, partitioner).step(workerStep).taskExecutor(batchTaskExecutor).gridSize(properties.getPartitionGridSize())` (confirmed via `javap`: `Partitioner`/`PartitionHandler`/`TaskExecutorPartitionHandler` live under `org.springframework.batch.core.partition(.support)`, not relocated like `Job`/`Step`/`JobExplorer` were). The actual chunk-processing steps are `ingestLineItemsWorkerStep`/`buildInvoicesWorkerStep`; the job's own wiring in `BatchJobConfig` is unchanged since the manager steps keep the original `ingestLineItemsStep`/`buildInvoicesStep` bean names. A shared `batchTaskExecutor` bean (`ThreadPoolTaskExecutor`, sized off `batch.partition-grid-size`, default 6) runs both — `TaskExecutorPartitionHandler` has no useful default executor, so omitting `.taskExecutor(...)` would silently run partitions sequentially. Raising `batch.partition-grid-size` resizes the thread pool automatically (same property), but not the DB connection pool — HikariCP's default max is 10, and each partition holds a connection for the duration of its chunk, so pushing grid size much past that would start serializing partitions on connection waits rather than actually running them in parallel; `spring.datasource.hikari.maximum-pool-size` would need to move too at that point.

Partitioning (not a multi-threaded step) was chosen deliberately: neither `FlatFileItemReader` nor the in-memory `DistinctOrderIdItemReader` is thread-safe, so a multi-threaded step would need a synchronizing wrapper around the reader, which serializes the read itself and only parallelizes processing/writing. Partitioning instead splits the *input* into disjoint ranges up front, so each partition gets its own reader instance and the entire read+process+write pipeline runs in parallel with no shared mutable state and no locking.

- **`LineRangePartitioner`** (`batch/step1/`): counts the CSV's data rows (total lines minus the header) and splits them into `gridSize` contiguous line ranges. Each partition's `ExecutionContext` carries `linesToSkip`/`maxItemCount`, consumed by `orderLineItemReader` (now `@StepScope`, late-bound via `@Value("#{stepExecutionContext['linesToSkip']}")` etc.) — confirmed via `javap` that `FlatFileItemReaderBuilder` has both `.linesToSkip(int)` and `.maxItemCount(int)`.
- **`OrderIdRangePartitioner`** (`batch/step2/`): range-based on the *sorted list* of distinct unprocessed order ids, not hash/modulo-based. Considered hashing by the staging table's numeric `id`, but rejected it — an order's line items aren't guaranteed to land in one contiguous `id` range, so an `id`-range split could split a single order's lines across two partitions. Slicing the already-sorted `orderId` list instead guarantees every order lands in exactly one partition (zero-padded ids like `ORD-000001` sort lexically the same as numerically, so a JPQL `between` on the string range matches the same slice). `OrderLineItemStagingRepository.findDistinctUnprocessedOrderIdsBetween(from, to)` backs this; `DistinctOrderIdItemReader` takes `fromOrderId`/`toOrderId` and falls back to an empty result if either is `null` (the "no unprocessed orders left" case, where the partitioner still emits at least one, empty, partition).
- **The one real hazard was `invoiceSummaryCsvItemWriter`**: a `FlatFileItemWriter`'s `open()`/`close()` lifecycle runs once per `StepExecution`, so leaving it a plain singleton would mean concurrent partitions calling `open()` on the *same* instance — corruption, independent of any write-level locking. Fixed by making it `@StepScope` with a per-partition output path (`invoice-summary-partition0.csv`, etc., derived from a `partitionKey` both partitioners also write into their `ExecutionContext`).
- **`mergeInvoiceSummaryStep`** (a plain `Tasklet`, not a chunk step — a one-shot file merge doesn't need chunk semantics) runs after `buildInvoicesStep` and recombines the per-partition files back into the single `invoice-summary.csv` the job is documented to produce, then deletes the partition files. `InvoiceSummaryMergeTasklet`/`InvoiceSummaryPartitionPaths` (`batch/step2/`) — the latter is shared by both the writer side (`BuildInvoicesStepConfig`) and the merge side so the two can never disagree on the naming scheme, and it discovers partition files by globbing the pattern rather than hardcoding a count, so it's correct regardless of `batch.partition-grid-size`. `perRunStateResetListener` (`BatchJobConfig`) also deletes any stale partition files in `beforeJob` — necessary because a prior run's leftover files (especially from a run with a *different* grid size) would otherwise get scooped up alongside the current run's fresh ones and corrupt the merge.
- **`CsvRejectedRecordSink` needed no changes** — both its `reset()` and `accept()` were already `synchronized`, so concurrent partitions writing to the shared `rejected-rows.csv` was already safe.
- **`FlakyOrderPersistenceSimulator` needed no changes either** — its `AtomicBoolean.compareAndSet` guard is inherently thread-safe, so it still fires exactly once per job run under concurrent partitions (whichever partition's writer thread wins the race), and that partition's retry policy still demonstrates the retry/rollback exactly as before.

### Operational control via JobOperator (stop / restart / abandon)

`launch()` in `BatchJobController` identifies each `JobInstance` by `businessDate` + `inputFile` `JobParameters` (see Idempotency below) rather than a random value, so it can't be used to resume a stopped or failed run — a second `launch()` call against an already-completed instance is rejected, it doesn't start a fresh one. `org.springframework.batch.core.launch.JobOperator` (package `core.launch`, confirmed via `javap`) fills that gap and is **already an available bean with zero extra config** — it's provided by `DefaultBatchConfiguration`, the machinery backing `@EnableBatchProcessing`, which `BatchJobConfig` already declares. `restart(executionId)` looks up the *original* failed/stopped execution's own `JobParameters` internally, which is why restart works correctly despite `launch()` always minting new ones — no parameter bookkeeping needed on the caller's side. `orderProcessingJob` never calls `.preventRestart()`, so it's restartable by default.

`JobControlService` (`batch/control/`) wraps `JobOperator` + `JobExplorer` behind three methods (`stop`, `restart`, `abandon`), each returning the existing `JobExecutionStatusResponse` DTO — re-fetching via `JobExplorer` after `stop`/`restart` since those return only a `boolean`/new execution id, not the execution itself (`abandon` returns the `JobExecution` directly). `BatchJobController`'s three new endpoints stay pure delegates, matching the existing thin-controller pattern. `BatchOperationExceptionHandler` (`web/`, `@RestControllerAdvice`) maps `JobOperator`'s checked exceptions to HTTP status codes instead of letting them fall through to a raw 500: `NoSuchJobExecutionException`/`NoSuchJobException` → 404, `JobExecutionAlreadyRunningException`/`JobExecutionNotRunningException`/`JobInstanceAlreadyCompleteException`/`JobRestartException`/`InvalidJobParametersException` → 409. Kept as a separate class (rather than local `@ExceptionHandler` methods on the controller) so the controller doesn't have to know about Batch's exception hierarchy at all.

If a restart ends up re-running `ingestLineItemsStep` mid-flight (rather than just resuming the already-completed `buildInvoicesStep`), the staging-table dedup check noted below under Idempotency applies here too — a restart won't re-insert lines already staged from the failed attempt.

### Idempotency (re-triggering the job)

Two layers guard against duplicate work on re-trigger:

- **Job-instance level**: `launch()` builds `JobParameters` from `businessDate` (today) + `inputFile` (`batch.inputCsvPath`), not a random value, so a `JobInstance` is identified by what it processed, not by when it was clicked. A second `POST` for the same business date against an already-completed instance is rejected outright (`JobInstanceAlreadyCompleteException` → 409) rather than launching a parallel run.
- **Row level, across different business dates**: `InvoiceAggregationProcessor` checks `orderRepository.existsByOrderNumber(orderId)` and returns `null` (Spring Batch filters nulls) for orders already invoiced. `OrderLineItemValidationProcessor` does the equivalent one step earlier, checking `OrderLineItemStagingRepository.existsByOrderIdAndProductId(orderId, productId)` before staging a row — regardless of that row's `processed` flag — so re-ingesting the same CSV (e.g. a re-run on a new business date against the unchanged demo file) no longer piles up duplicate staging rows the way it used to.

**Assumptions and remaining gaps, worth knowing before extending this:**
- The `(orderId, productId)` dedup key assumes a domain rule that an order has at most one line item per product. If that's ever relaxed (the same product legitimately appearing as two separate line entries on one order), this key can no longer distinguish a genuine second entry from a resend — it would need a line number or source-record id instead.
- It detects and drops exact re-deliveries; it does **not** reconcile corrections. A resend of the same `(orderId, productId)` with a different `quantity`/`unitPrice` is treated as a duplicate and silently dropped — the originally staged values win.
- The `existsBy` check is check-then-act, not atomic — safe under this job's own partitioning (each partition reads a disjoint CSV line range, so the same row is never processed twice within one run) but not a substitute for a DB-level unique constraint if two full job runs could ever execute concurrently against overlapping data.

### Validation is Open/Closed on purpose

`LineItemValidationRule` (`batch/validation/`) is an interface; each business rule (`PositiveQuantityRule`, `PositiveUnitPriceRule`, `NonBlankCustomerNameRule`, `NonBlankOrderIdRule`) is its own `@Component`. `DefaultOrderLineValidator` constructor-injects `List<LineItemValidationRule>` (Spring auto-collects every implementation) and aggregates all violations into one `InvalidOrderLineException`. Add a new rule by adding a new class — nothing else changes.

### Config properties (`batch.*` prefix, `config/BatchProperties`)

`inputCsvPath`, `rejectsFilePath`, `invoiceSummaryOutputPath`, `taxRate`, `chunkSize`, `skipLimit`, `retryLimit`, `simulateTransientWriteFailures` — bound via `@ConfigurationProperties`, registered with `@EnableConfigurationProperties(BatchProperties.class)` on `DemoApplication`.

### Demo-only shortcuts (would not survive contact with production)

- `spring.jpa.hibernate.ddl-auto=update` — no migration tool (Flyway/Liquibase) is wired up.
- `spring.sql.init.mode=always` with `continue-on-error=true` for the Batch schema — real usage would use a proper migration instead of re-running non-idempotent DDL on every boot.
- Grafana runs with `GF_AUTH_ANONYMOUS_ENABLED=true` (viewer role) and default `admin`/`admin` credentials — open dashboard access with no login prompt, fine for `localhost`-only use, would need real auth before being reachable by anyone else.
