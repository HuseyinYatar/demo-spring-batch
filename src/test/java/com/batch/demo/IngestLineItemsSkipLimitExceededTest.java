package com.batch.demo;

import java.time.LocalDate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import com.batch.demo.repository.OrderLineItemStagingRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;
import com.batch.demo.testsupport.StepExecutions;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * batch.skip-limit is 5 (application-test.properties); this fixture has 7 skippable
 * rows, so the step must fail outright once the limit is exceeded rather than
 * silently absorbing every bad row.
 */
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = "batch.input-csv-path=classpath:data/test-order-line-items-skip-limit-exceeded.csv")
@SpringBatchTest
class IngestLineItemsSkipLimitExceededTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private OrderLineItemStagingRepository stagingRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void exceedingSkipLimitFailsTheStep() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("businessDate", LocalDate.now())
                .addString("inputFile", "classpath:data/test-order-line-items-skip-limit-exceeded.csv")
                .toJobParameters();

        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);

        StepExecution ingestPartition = StepExecutions.named(execution, "ingestLineItemsWorkerStep:partition0");
        assertThat(ingestPartition.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(ingestPartition.getFailureExceptions())
                .anyMatch(SkipLimitExceededException.class::isInstance);

        assertThat(stagingRepository.count()).isZero();
    }
}
