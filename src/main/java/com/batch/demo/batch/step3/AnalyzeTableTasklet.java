package com.batch.demo.batch.step3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Runs {@code ANALYZE <table>} so Postgres has planner statistics for rows a previous job
 * has just bulk-loaded. Without them the planner guesses a handful of matching rows for
 * the report's {@code issued_date = ?} filter (all 71k invoices of a day actually match),
 * picks "scan the table, then top-N sort" and pays that on every page of the detail
 * reader. Autovacuum would analyze the table too, but only after its next wake-up
 * (about a minute) - after dailyPipelineJob's report has already finished. It costs
 * ~0.1 s for 71k rows and cut the detail step from 9.4 s to 3.9 s.
 *
 * Deliberately not guarded: a failure here (e.g. the application user not owning the table)
 * fails the job like any other step instead of being swallowed. ANALYZE runs inside the
 * step's transaction, and a failed statement would leave that transaction aborted anyway.
 * Idempotent, so a restart of a failed run can simply repeat it.
 */
public class AnalyzeTableTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(AnalyzeTableTasklet.class);

    private final JdbcTemplate jdbcTemplate;
    private final String table;

    /**
     * @param table an unquoted table name; checked because it is concatenated into the
     *              statement (ANALYZE takes no bind parameters)
     */
    public AnalyzeTableTasklet(JdbcTemplate jdbcTemplate, String table) {
        if (table == null || !table.matches("[a-z_][a-z0-9_]*")) {
            throw new IllegalArgumentException("Not a plain table name: " + table);
        }
        this.jdbcTemplate = jdbcTemplate;
        this.table = table;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        long start = System.nanoTime();
        jdbcTemplate.execute("ANALYZE " + table);
        log.info("Analyzed table {} in {} ms", table, (System.nanoTime() - start) / 1_000_000);
        return RepeatStatus.FINISHED;
    }
}
