package com.batch.demo.maintenance;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import com.batch.demo.config.BatchProperties;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;
import com.batch.demo.testsupport.BusinessDataSeeder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the runner does when it runs. That it runs at startup, and before anything else,
 * is TruncateBusinessTablesOnStartupTest's job - this class has the startup run switched
 * off (application-test.properties) and calls the runner itself.
 */
@SpringBootTest
@ActiveProfiles("test")
class TruncateBusinessTablesRunnerTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void emptiesEveryBusinessTableAndRestartsTheIdentityColumns() {
        BusinessDataSeeder.seed(jdbcTemplate);
        for (String table : TruncateBusinessTablesRunner.BUSINESS_TABLES) {
            assertThat(rows(table)).as("%s before", table).isPositive();
        }

        runner(true).run();

        for (String table : TruncateBusinessTablesRunner.BUSINESS_TABLES) {
            assertThat(rows(table)).as("%s after", table).isZero();
        }
        // The seeded staging row had id 1; without RESTART IDENTITY the next one would get 2.
        assertThat(BusinessDataSeeder.insertStagingRow(jdbcTemplate)).isEqualTo(1L);
    }

    @Test
    void leavesTheBusinessTablesAloneWhenTheFlagIsOff() {
        BusinessDataSeeder.seed(jdbcTemplate);

        runner(false).run();

        for (String table : TruncateBusinessTablesRunner.BUSINESS_TABLES) {
            assertThat(rows(table)).as(table).isEqualTo(1);
        }
    }

    /**
     * The list is an explicit allowlist, so a new entity's table would silently never be
     * emptied. Everything in the schema that is not a Spring Batch table has to be on it.
     */
    @Test
    void theTableListCoversEveryTableThatIsNotASpringBatchTable() {
        List<String> tables = jdbcTemplate.queryForList("""
                select table_name from information_schema.tables
                where table_schema = 'public' and table_type = 'BASE TABLE' and table_name not like 'batch\\_%'
                """, String.class);

        assertThat(TruncateBusinessTablesRunner.BUSINESS_TABLES).containsExactlyInAnyOrderElementsOf(tables);
    }

    private TruncateBusinessTablesRunner runner(boolean enabled) {
        BatchProperties properties = new BatchProperties();
        properties.setTruncateBusinessTablesOnStartup(enabled);
        return new TruncateBusinessTablesRunner(jdbcTemplate, properties);
    }

    private long rows(String table) {
        Long count = jdbcTemplate.queryForObject("select count(*) from " + table, Long.class);
        return count == null ? 0 : count;
    }
}
