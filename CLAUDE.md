# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Spring Batch demo: "Daily E-commerce Order & Invoice Processing". Ingests a CSV of order line items, validates and aggregates them into invoices, persists results to PostgreSQL, writes a CSV summary, and demonstrates a real skip-and-log error-handling path (not a happy-path-only toy).

Stack: Spring Boot 4.1.1 (Java 21), Spring Batch (bundled Spring Batch 6.0.5), Spring Data JPA / Hibernate, Spring Web, Lombok, PostgreSQL.

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

## Architecture

### Spring Batch 6.0 package relocations (important, non-obvious)

This project's Spring Batch version (6.0.5, bundled with Boot 4.1.1) relocated most classes from their pre-6.0 packages. Get these wrong and the build fails with "package does not exist" even though the dependency is on the classpath:

- `org.springframework.batch.item.*` → `org.springframework.batch.infrastructure.item.*` (`ItemReader`, `ItemWriter`, `ItemProcessor`, `Chunk`, `ExecutionContext`, `FlatFileItemReader`/`Writer`, `FieldSet`, `FieldSetMapper`, `FieldExtractor`, `JpaItemWriter`, `CompositeItemWriter`, `AbstractItemStreamItemReader`, etc.)
- `org.springframework.batch.core.*` split into subpackages: `Job`/`JobInstance` → `core.job`, `JobExecution` → `core.job`, `JobParameters`/`JobParametersBuilder` → `core.job.parameters`, `Step`/`StepExecution` → `core.step`, `JobExecutionListener`/`SkipListener` → `core.listener`, `JobExplorer` → `core.repository.explore`, `JobExecutionAlreadyRunningException`/`JobRestartException`/`JobInstanceAlreadyCompleteException` → `core.launch`, `JobParametersInvalidException` was renamed to `InvalidJobParametersException` and moved to `core.job.parameters`.
- `JobBuilder`, `StepBuilder`, `JobRepository`, `JobLauncher`, `@StepScope` kept their pre-6.0 package locations.

### JobRepository must be explicitly JDBC-backed

Boot 4.1's batch auto-configuration (`BatchAutoConfiguration`) applies only `@EnableBatchProcessing` with no `@EnableJdbcJobRepository`, so **by default Spring Batch uses an in-memory `JobRepository`** even with a `DataSource` present — `spring.batch.jdbc.initialize-schema` no longer exists as a property in this version (Boot's `BatchProperties` only has a `job` sub-property now).

To get real, persistent job/step execution history in Postgres, `BatchJobConfig` explicitly declares both `@EnableBatchProcessing` and `@EnableJdbcJobRepository`. This works because Boot's own `BatchAutoConfiguration` backs off (`@ConditionalOnMissingBean(annotation = EnableBatchProcessing.class)`) once any other `@EnableBatchProcessing`-annotated class is found — declaring `@EnableJdbcJobRepository` alone, without also declaring `@EnableBatchProcessing` on the same class, is silently ignored. The `BATCH_*` schema itself is created via Boot's generic SQL initializer, pointed at Spring Batch's bundled DDL: `spring.sql.init.schema-locations=classpath:org/springframework/batch/core/schema-postgresql.sql` in `application.properties`. `spring.sql.init.continue-on-error=true` is set because that DDL isn't idempotent (no `IF NOT EXISTS`), so repeated app restarts against the same DB would otherwise fail on the second boot.

### `@StepScope` readers must be typed by their concrete class, not the `ItemReader` interface

A `@Bean @StepScope` method that returns the `ItemReader<T>` interface type causes the framework to never invoke `open()`/`close()` on it (Spring Batch logs a warning about this: "If using @StepScope on a @Bean method, be sure to return the implementing class"). Since `DistinctOrderIdItemReader` (see below) holds iteration state that's only populated in `open()`, getting this wrong makes the step silently process zero items with no error. Always declare the `@Bean` method's return type as the concrete class (see `BuildInvoicesStepConfig.distinctOrderIdItemReader`).

### Job design: two steps, staging table in between

`orderProcessingJob` = `ingestLineItemsStep` → `buildInvoicesStep`. A single-step "aggregate consecutive CSV rows into one order" reader was deliberately avoided because it would require assuming the CSV is pre-sorted by `orderId` and would complicate per-item skip semantics.

- **`ingestLineItemsStep`** (`batch/step1/`, wired in `config/IngestLineItemsStepConfig`): reads the CSV row-by-row into `OrderLineCsvRecord`, validates each row independently, and writes valid ones into the flat `OrderLineItemStaging` entity (no FK relations — just a landing table with a `processed` flag). This is where all per-row error handling happens, since Spring Batch's skip API operates at the single-item level:
  - Malformed lines (bad column count, non-numeric price, bad date) → `FlatFileParseException` → skip on read.
  - Business-rule violations (negative qty/price, blank customer name, blank orderId) → `InvalidOrderLineException` → skip on process.
  - Both registered via `.faultTolerant().skip(...).skipLimit(...)`; a `RejectedRecordSkipListener` logs the raw record + reason to `rejected-rows.csv` via the `RejectedRecordSink` abstraction (`CsvRejectedRecordSink`).
- **`buildInvoicesStep`** (`batch/step2/`, wired in `config/BuildInvoicesStepConfig`): `DistinctOrderIdItemReader` reads distinct unprocessed `orderId`s from staging; `InvoiceAggregationProcessor` loads all staged lines for an id, builds the real `Order`/`OrderLineItem` entities, and delegates tax/total math to `InvoiceCalculator`. The `CompositeItemWriter` fans each result out to three single-purpose delegates: `OrderPersistenceItemWriter` (JPA save, cascades to line items + invoice), the `FlatFileItemWriter` for `invoice-summary.csv`, and `StagingMarkProcessedItemWriter` (flips `processed=true` in the same chunk transaction).

### Idempotency (re-triggering the job)

The REST endpoint can be called more than once. `InvoiceAggregationProcessor` checks `orderRepository.existsByOrderNumber(orderId)` and returns `null` (Spring Batch filters nulls — no exception needed) for orders already invoiced by a prior run. **Known accepted limitation**: `ingestLineItemsStep` has no such guard, so re-running the job re-inserts duplicate staging rows for orders already invoiced; these stay `processed=false` forever since `buildInvoicesStep` filters them out before ever loading them. Harmless, but don't be surprised by growing "unprocessed" staging rows across repeated demo runs against the same DB.

### Validation is Open/Closed on purpose

`LineItemValidationRule` (`batch/validation/`) is an interface; each business rule (`PositiveQuantityRule`, `PositiveUnitPriceRule`, `NonBlankCustomerNameRule`, `NonBlankOrderIdRule`) is its own `@Component`. `DefaultOrderLineValidator` constructor-injects `List<LineItemValidationRule>` (Spring auto-collects every implementation) and aggregates all violations into one `InvalidOrderLineException`. Add a new rule by adding a new class — nothing else changes.

### Config properties (`batch.*` prefix, `config/BatchProperties`)

`inputCsvPath`, `rejectsFilePath`, `invoiceSummaryOutputPath`, `taxRate`, `chunkSize`, `skipLimit` — bound via `@ConfigurationProperties`, registered with `@EnableConfigurationProperties(BatchProperties.class)` on `DemoApplication`.

### Demo-only shortcuts (would not survive contact with production)

- `spring.jpa.hibernate.ddl-auto=update` — no migration tool (Flyway/Liquibase) is wired up.
- `spring.sql.init.mode=always` with `continue-on-error=true` for the Batch schema — real usage would use a proper migration instead of re-running non-idempotent DDL on every boot.
