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
```

There is no separate lint step; Lombok annotation processing runs as part of `compile`/`testCompile` in the `maven-compiler-plugin` execution in `pom.xml`.

`DemoApplicationTests` (`@SpringBootTest`) loads the full application context and therefore requires Postgres to be running (`docker compose up -d` first) — it will fail otherwise. `DefaultOrderLineValidatorTest` and `InvoiceCalculatorTest` are plain unit tests with no Spring context / DB dependency.

### Triggering the job manually

```bash
curl -X POST http://localhost:8080/api/batch/jobs/order-processing
curl http://localhost:8080/api/batch/jobs/executions/{id}
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

`orderProcessingJob` = `ingestLineItemsStep` → `buildInvoicesStep`. A single-step "aggregate consecutive CSV rows into one order" reader was deliberately avoided because it would require assuming the CSV is pre-sorted by `orderId` and would complicate per-item skip semantics.

- **`ingestLineItemsStep`** (`batch/step1/`, wired in `config/IngestLineItemsStepConfig`): reads the CSV row-by-row into `OrderLineCsvRecord`, validates each row independently, and writes valid ones into the flat `OrderLineItemStaging` entity (no FK relations — just a landing table with a `processed` flag). This is where all per-row error handling happens, since Spring Batch's skip API operates at the single-item level:
  - Malformed lines (bad column count, non-numeric price, bad date) → `FlatFileParseException` → skip on read.
  - Business-rule violations (negative qty/price, blank customer name, blank orderId) → `InvalidOrderLineException` → skip on process.
  - Both registered via `.faultTolerant().skip(...).skipLimit(...)`; a `RejectedRecordSkipListener` logs the raw record + reason to `rejected-rows.csv` via the `RejectedRecordSink` abstraction (`CsvRejectedRecordSink`).
- **`buildInvoicesStep`** (`batch/step2/`, wired in `config/BuildInvoicesStepConfig`): `DistinctOrderIdItemReader` reads distinct unprocessed `orderId`s from staging; `InvoiceAggregationProcessor` loads all staged lines for an id, builds the real `Order`/`OrderLineItem` entities, and delegates tax/total math to `InvoiceCalculator`. The `CompositeItemWriter` fans each result out to three single-purpose delegates: `OrderPersistenceItemWriter` (JPA save, cascades to line items + invoice), the `FlatFileItemWriter` for `invoice-summary.csv`, and `StagingMarkProcessedItemWriter` (flips `processed=true` in the same chunk transaction).

### Retry is simulated on purpose, the same way skip uses deliberately-bad CSV rows

`buildInvoicesStep` also demonstrates `.faultTolerant().retry(...)` — a genuinely transient exception (a dropped DB connection, a lock timeout) would never actually fire against a healthy local Postgres, so `FlakyOrderPersistenceSimulator` (`batch/step2/`) throws `TransientInvoiceWriteException` exactly once per job run, from the top of `OrderPersistenceItemWriter.write(...)`, before anything in the chunk touches the DB. The chunk transaction rolls back cleanly (nothing partially written), Spring Batch retries the same chunk — re-running `InvoiceAggregationProcessor.process(...)` for its buffered items too, which is safe because that processor already re-checks `existsByOrderNumber` — and the retry succeeds because the simulator's flag is now tripped. `perRunStateResetListener` (in `BatchJobConfig`, the same `beforeJob` listener that resets the rejects file) re-arms it every run via `flakySimulator.reset()`, so every run shows exactly one retry, deterministically. `InvoiceWriteRetryListener` logs a `WARN` on each retry attempt so it's visible in the console. Toggle off with `batch.simulate-transient-write-failures=false`; retry count via `batch.retry-limit` (default 3).

`TransientInvoiceWriteException` is *also* registered as skippable (`.skip(TransientInvoiceWriteException.class).skipLimit(properties.getSkipLimit())`, reusing the same `batch.skip-limit` step 1 uses), so the two policies compose the way Spring Batch intends: retry first, and only if `retryLimit` attempts are all exhausted does Spring Batch scan the chunk item-by-item and skip the one offending item instead of failing the whole step/job. In the current deterministic simulation this path never actually triggers (the simulator always succeeds by the 2nd attempt), but it's there so a *genuinely* persistent transient failure degrades gracefully instead of failing the job outright. Note: unlike step 1, no `SkipListener` is registered here, so a skip on this path is currently silent (no audit trail) — `readCount`/`writeCount`/`skipCount` on the `buildInvoicesStep` `StepExecution` would still reflect it, just nothing gets written to a file.

### Idempotency (re-triggering the job)

The REST endpoint can be called more than once. `InvoiceAggregationProcessor` checks `orderRepository.existsByOrderNumber(orderId)` and returns `null` (Spring Batch filters nulls — no exception needed) for orders already invoiced by a prior run. **Known accepted limitation**: `ingestLineItemsStep` has no such guard, so re-running the job re-inserts duplicate staging rows for orders already invoiced; these stay `processed=false` forever since `buildInvoicesStep` filters them out before ever loading them. Harmless, but don't be surprised by growing "unprocessed" staging rows across repeated demo runs against the same DB.

### Validation is Open/Closed on purpose

`LineItemValidationRule` (`batch/validation/`) is an interface; each business rule (`PositiveQuantityRule`, `PositiveUnitPriceRule`, `NonBlankCustomerNameRule`, `NonBlankOrderIdRule`) is its own `@Component`. `DefaultOrderLineValidator` constructor-injects `List<LineItemValidationRule>` (Spring auto-collects every implementation) and aggregates all violations into one `InvalidOrderLineException`. Add a new rule by adding a new class — nothing else changes.

### Config properties (`batch.*` prefix, `config/BatchProperties`)

`inputCsvPath`, `rejectsFilePath`, `invoiceSummaryOutputPath`, `taxRate`, `chunkSize`, `skipLimit`, `retryLimit`, `simulateTransientWriteFailures` — bound via `@ConfigurationProperties`, registered with `@EnableConfigurationProperties(BatchProperties.class)` on `DemoApplication`.

### Demo-only shortcuts (would not survive contact with production)

- `spring.jpa.hibernate.ddl-auto=update` — no migration tool (Flyway/Liquibase) is wired up.
- `spring.sql.init.mode=always` with `continue-on-error=true` for the Batch schema — real usage would use a proper migration instead of re-running non-idempotent DDL on every boot.
- Grafana runs with `GF_AUTH_ANONYMOUS_ENABLED=true` (viewer role) and default `admin`/`admin` credentials — open dashboard access with no login prompt, fine for `localhost`-only use, would need real auth before being reachable by anyone else.
