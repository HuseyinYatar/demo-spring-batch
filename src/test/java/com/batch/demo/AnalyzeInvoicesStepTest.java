package com.batch.demo;

import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import com.batch.demo.batch.step3.AnalyzeTableTasklet;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * dailySalesReportJob analyzes the invoice table first, because right after the order job's
 * bulk load Postgres has no statistics for it and the report's queries get a bad plan (see
 * AnalyzeTableTasklet). Nothing functional breaks if that step disappears - the report just
 * gets slower - so it is pinned here.
 */
@SpringBootTest
@ActiveProfiles("test")
class AnalyzeInvoicesStepTest extends AbstractPostgresIntegrationTest {

    private static final LocalDate DAY = LocalDate.of(2099, 4, 1);

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("orderProcessingJob")
    private Job orderProcessingJob;

    @Autowired
    @Qualifier("dailySalesReportJob")
    private Job dailySalesReportJob;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void theReportJobAnalyzesTheInvoiceTableBeforeReadingItAndTheStatisticsAppear() throws Exception {
        // Autovacuum off, so any statistics seen below can only come from the job's own ANALYZE.
        jdbcTemplate.execute("alter table invoice set (autovacuum_enabled = false)");
        assertThat(jobLauncher.run(orderProcessingJob, new JobParametersBuilder()
                .addLocalDate("businessDate", DAY)
                .addString("inputFile", "classpath:data/test-order-line-items.csv")
                .toJobParameters()).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(columnStatisticsRows("invoice")).as("statistics before the report job").isZero();

        JobExecution report = jobLauncher.run(dailySalesReportJob, new JobParametersBuilder()
                .addLocalDate("businessDate", DAY)
                .addLong("runId", 1L)
                .toJobParameters());

        assertThat(report.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        List<StepExecution> inOrder = report.getStepExecutions().stream()
                .sorted(Comparator.comparing(StepExecution::getId)).toList();
        assertThat(inOrder).extracting(StepExecution::getStepName)
                .containsExactly("analyzeInvoicesStep", "dailyInvoiceDetailStep", "dailySalesSummaryStep");
        assertThat(inOrder.get(0).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(columnStatisticsRows("invoice")).as("statistics after the report job").isPositive();
    }

    @Test
    void theTaskletOnlyAcceptsAPlainTableName() {
        assertThatThrownBy(() -> new AnalyzeTableTasklet(jdbcTemplate, "invoice; drop table orders"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AnalyzeTableTasklet(jdbcTemplate, "Invoice"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AnalyzeTableTasklet(jdbcTemplate, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private long columnStatisticsRows(String table) {
        Long count = jdbcTemplate.queryForObject(
                "select count(*) from pg_stats where schemaname = 'public' and tablename = ?", Long.class, table);
        return count == null ? 0 : count;
    }
}
