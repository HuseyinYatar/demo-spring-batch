package com.batch.demo.maintenance;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.batch.demo.config.BatchProperties;

import lombok.RequiredArgsConstructor;

/**
 * Empties the business tables when the application starts, so every start begins from
 * an empty database (gated by {@code batch.truncate-business-tables-on-startup}).
 * <p>
 * Only the tables behind the JPA entities are touched. Spring Batch's own {@code BATCH_*}
 * job-repository tables are left alone on purpose, which has two consequences:
 * <ul>
 * <li>execution history (and the Grafana dashboards built on it) survives a restart;</li>
 * <li>a {@code JobInstance} that already completed for today's {@code businessDate} still
 * exists, so triggering the same job again without {@code ?businessDate=} is still
 * rejected with 409 even though the tables are empty again.</li>
 * </ul>
 * Runs at order 0, i.e. before any runner without an explicit {@code @Order} (those sort
 * last), so a later runner never sees rows this one is about to delete. By then
 * {@code ddl-auto=update} has created the tables, so a first start against an empty
 * database truncates empty tables.
 */
@Component
@Order(0)
@RequiredArgsConstructor
public class TruncateBusinessTablesRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(TruncateBusinessTablesRunner.class);

    /**
     * Every table mapped by a JPA entity - an explicit allowlist rather than "whatever is
     * in the schema", so a table nobody listed here is never emptied. Add new entity
     * tables here ({@code TruncateBusinessTablesRunnerTest} fails when one is missing).
     * <p>
     * They go into a single {@code TRUNCATE} without {@code CASCADE}: Postgres accepts
     * foreign-key-linked tables when all of them are named, and a table that references
     * one of these but is not listed makes the statement fail instead of being silently
     * emptied along with them.
     */
    static final List<String> BUSINESS_TABLES = List.of(
            "invoice",
            "order_line_item",
            "orders",
            "order_line_item_staging",
            "daily_sales_report");

    private final JdbcTemplate jdbcTemplate;
    private final BatchProperties properties;

    @Override
    public void run(String... args) {
        if (!properties.isTruncateBusinessTablesOnStartup()) {
            log.debug("batch.truncate-business-tables-on-startup is off; leaving business tables as they are");
            return;
        }
        // RESTART IDENTITY resets the id sequences owned by identity columns (staging and
        // daily_sales_report). The SEQUENCE-generated ids (orders_seq etc.) keep counting,
        // which is harmless: nothing depends on ids starting from 1.
        jdbcTemplate.execute("TRUNCATE TABLE " + String.join(", ", BUSINESS_TABLES) + " RESTART IDENTITY");
        log.warn("Truncated business tables {} on startup (batch.truncate-business-tables-on-startup=true)",
                BUSINESS_TABLES);
    }
}
