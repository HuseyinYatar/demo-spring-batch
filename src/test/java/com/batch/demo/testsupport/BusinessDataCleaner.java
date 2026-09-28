package com.batch.demo.testsupport;

import org.springframework.jdbc.core.JdbcTemplate;

public final class BusinessDataCleaner {

    private BusinessDataCleaner() {
    }

    /**
     * Also truncates BATCH_* job-repository tables: within one test class, the
     * Testcontainers Postgres is shared across all @Test methods (the container field
     * is per-class, not per-method), so without this a second method's job launch can
     * collide with a completed JobInstance a prior method already created for the
     * same identifying parameters (businessDate + inputFile).
     */
    public static void truncateAll(JdbcTemplate jdbc) {
        jdbc.execute("TRUNCATE TABLE invoice, order_line_item, orders, order_line_item_staging RESTART IDENTITY CASCADE");
        jdbc.execute("TRUNCATE TABLE batch_job_execution_context, batch_step_execution_context, "
                + "batch_step_execution, batch_job_execution_params, batch_job_execution, batch_job_instance "
                + "RESTART IDENTITY CASCADE");
    }
}
