package com.batch.demo.maintenance;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataSeeder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The real startup path: rows that are already there while the application starts (a seeding
 * runner stands in for the previous run's data; the tables exist by then because
 * {@code ddl-auto=update} runs before any runner) are gone once startup has finished, and
 * Spring Batch's own tables are not touched.
 * <p>
 * The seeded {@code batch_job_instance} row doubles as proof that the seeding runner ran at
 * all - without it, "business tables are empty" would also pass if seeding had silently not
 * happened, or had happened after the truncation.
 */
@SpringBootTest(properties = "batch.truncate-business-tables-on-startup=true")
@ActiveProfiles("test")
@Import(TruncateBusinessTablesOnStartupTest.SeedBeforeTruncation.class)
class TruncateBusinessTablesOnStartupTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void rowsPresentWhenTheApplicationStartsAreGoneOnceItIsUp() {
        assertThat(rows("batch_job_instance")).as("seeding runner ran, BATCH_* untouched").isEqualTo(1);
        for (String table : TruncateBusinessTablesRunner.BUSINESS_TABLES) {
            assertThat(rows(table)).as(table).isZero();
        }
    }

    private long rows(String table) {
        Long count = jdbcTemplate.queryForObject("select count(*) from " + table, Long.class);
        return count == null ? 0 : count;
    }

    /** Sorts before the truncating runner (order 0). */
    @TestComponent
    @Order(-1)
    static class SeedBeforeTruncation implements CommandLineRunner {

        private final JdbcTemplate jdbcTemplate;

        SeedBeforeTruncation(JdbcTemplate jdbcTemplate) {
            this.jdbcTemplate = jdbcTemplate;
        }

        @Override
        public void run(String... args) {
            BusinessDataSeeder.seed(jdbcTemplate);
            jdbcTemplate.update("insert into batch_job_instance (job_instance_id, version, job_name, job_key) "
                    + "values (1, 0, 'seedJob', 'seedKey')");
        }
    }
}
