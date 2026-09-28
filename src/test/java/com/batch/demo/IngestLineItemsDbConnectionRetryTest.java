package com.batch.demo;

import java.time.LocalDate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import com.batch.demo.repository.OrderLineItemStagingRepository;
import com.batch.demo.repository.OrderRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;
import com.batch.demo.testsupport.StepExecutions;
import com.batch.demo.testsupport.fault.SqlInsertFaultTrigger;
import com.batch.demo.testsupport.fault.StagingConnectionFaultInjectionTestConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves ingestLineItemsWorkerStep's new retry registration
 * (TransientDataAccessException / DataAccessResourceFailureException, added
 * alongside buildInvoicesStep's) actually engages for a genuine, correctly-classified
 * DB connection failure - not just for FlakyOrderPersistenceSimulator's own synthetic
 * exception type, which is unrelated to real Spring/Hibernate exception translation.
 */
@SpringBootTest
@ActiveProfiles("test")
@Import(StagingConnectionFaultInjectionTestConfig.class)
@SpringBatchTest
class IngestLineItemsDbConnectionRetryTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private SqlInsertFaultTrigger trigger;

    @Autowired
    private OrderLineItemStagingRepository stagingRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
        trigger.disarm();
    }

    @Test
    void connectionFailureDuringIngestRecoversViaRetry() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("businessDate", LocalDate.now())
                .addString("inputFile", "classpath:data/test-order-line-items.csv")
                .toJobParameters();

        // chunk-size=2: rows 1-2 commit, row 3's insert throws the injected
        // SQLTransientConnectionException, classified by Hibernate/Spring as
        // DataAccessResourceFailureException - now retryable, so the retried attempt
        // (trigger already fired once) succeeds and the job completes normally.
        trigger.arm(2);
        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(stagingRepository.count()).isEqualTo(7);
        assertThat(stagingRepository.findAll()).allSatisfy(row -> assertThat(row.isProcessed()).isTrue());
        assertThat(orderRepository.count()).isEqualTo(6);

        StepExecution ingestPartition = StepExecutions.named(execution, "ingestLineItemsWorkerStep:partition0");
        assertThat(ingestPartition.getRollbackCount()).isEqualTo(1);
        assertThat(ingestPartition.getSkipCount()).isZero();
        assertThat(ingestPartition.getWriteCount()).isEqualTo(7);
    }
}
