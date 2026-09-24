package com.batch.demo;

import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import com.batch.demo.batch.control.JobControlService;
import com.batch.demo.domain.OrderLineItemStaging;
import com.batch.demo.repository.OrderLineItemStagingRepository;
import com.batch.demo.repository.OrderRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;
import com.batch.demo.testsupport.StepExecutions;
import com.batch.demo.testsupport.fault.SqlInsertFaultTrigger;
import com.batch.demo.testsupport.fault.StagingFaultInjectionTestConfig;
import com.batch.demo.web.dto.JobExecutionStatusResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * OrderProcessingJobFailureRestartTest only exercises restart of buildInvoicesStep,
 * which resumes purely off DB state (the "processed" flag) - a completely different
 * mechanism from ingestLineItemsStep's FlatFileItemReader, which has its own
 * line-position restart tracking via ExecutionContext, layered on top of
 * LineRangePartitioner recomputing the same full-file line ranges on every launch
 * including restart. This test exists to verify that combination doesn't silently
 * drop or duplicate rows - CLAUDE.md documents the expectation (the
 * existsByOrderIdAndProductId dedup guard should cover this) but until this test, it
 * was never actually run.
 */
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = "batch.input-csv-path=classpath:data/test-order-line-items-step1-restart.csv")
@Import(StagingFaultInjectionTestConfig.class)
@SpringBatchTest
class IngestLineItemsRestartTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private JobControlService jobControlService;

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
    void restartAfterIngestFailureDoesNotDuplicateOrMissRows() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("businessDate", LocalDate.now())
                .addString("inputFile", "classpath:data/test-order-line-items-step1-restart.csv")
                .addLong("startedAtEpochMs", System.currentTimeMillis(), false)
                .toJobParameters();

        // Phase 1: fail partway through ingestLineItemsStep - chunk 1 (M01, M02,
        // chunk-size=2) commits, chunk 2's first insert (M03) is the injected fault.
        trigger.arm(2);
        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        StepExecution failedPartition = StepExecutions.named(execution, "ingestLineItemsWorkerStep:partition0");
        assertThat(failedPartition.getStatus()).isEqualTo(BatchStatus.FAILED);

        assertThat(stagingRepository.findAll()).extracting(OrderLineItemStaging::getOrderId)
                .containsExactlyInAnyOrder("ORD-M01", "ORD-M02");
        assertThat(orderRepository.count()).isZero();

        // Phase 2: recover and restart - the real question this test answers.
        trigger.disarm();
        JobExecutionStatusResponse restarted = jobControlService.restart(execution.getId());
        JobExecution restartedExecution = jobExplorer.getJobExecution(restarted.jobExecutionId());

        assertThat(restartedExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        List<OrderLineItemStaging> stagingRows = stagingRepository.findAll();
        assertThat(stagingRows).extracting(OrderLineItemStaging::getOrderId)
                .containsExactlyInAnyOrder(
                        "ORD-M01", "ORD-M02", "ORD-M03", "ORD-M04", "ORD-M05", "ORD-M06");
        assertThat(stagingRows).hasSize(6); // no duplicates alongside the 6 distinct ids
        assertThat(stagingRows).allSatisfy(row -> assertThat(row.isProcessed()).isTrue());
        assertThat(orderRepository.count()).isEqualTo(6);
    }
}
