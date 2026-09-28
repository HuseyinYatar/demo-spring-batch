package com.batch.demo;

import java.time.LocalDate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.batch.demo.batch.control.JobControlService;
import com.batch.demo.repository.OrderRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;
import com.batch.demo.testsupport.RunningJobs;
import com.batch.demo.testsupport.slow.SlowStagingInsertTrigger;
import com.batch.demo.testsupport.slow.SlowingDataSourceTestConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * JobLauncher in this app is synchronous (confirmed by every other test blocking on
 * launchJob()), so stopping a running job requires launching it on a background
 * thread and racing the test's main thread against it - SlowStagingInsertTrigger
 * exists purely to widen that race window deterministically (the tiny fixtures here
 * otherwise complete in well under the time it takes to launch async and poll for
 * STARTED). See RunningJobs for why the launch/stop orchestration itself lives outside
 * this test class rather than as a private helper here.
 */
@SpringBootTest
@ActiveProfiles("test")
@Import(SlowingDataSourceTestConfig.class)
@SpringBatchTest
class JobControlOperationsTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private JobControlService jobControlService;

    @Autowired
    private SlowStagingInsertTrigger trigger;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
        trigger.disarm();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void stoppingARunningJobTransitionsToStopped() throws Exception {
        JobExecution finished = RunningJobs.launchAndStop(
                jobLauncherTestUtils, jobExplorer, jobControlService, executor, newJobParameters());

        assertThat(finished.getStatus()).isEqualTo(BatchStatus.STOPPED);
        // ingestLineItemsStep was still mid-flight when stopped, so buildInvoicesStep
        // (and therefore any Order) must never have started.
        assertThat(orderRepository.count()).isZero();
    }

    @Test
    void abandoningAStoppedExecutionMarksItAbandoned() throws Exception {
        JobExecution finished = RunningJobs.launchAndStop(
                jobLauncherTestUtils, jobExplorer, jobControlService, executor, newJobParameters());
        assertThat(finished.getStatus()).isEqualTo(BatchStatus.STOPPED);

        var abandoned = jobControlService.abandon(finished.getId());

        assertThat(abandoned.status()).isEqualTo(BatchStatus.ABANDONED.name());
        assertThat(jobExplorer.getJobExecution(finished.getId()).getStatus()).isEqualTo(BatchStatus.ABANDONED);
    }

    private JobParameters newJobParameters() {
        trigger.arm(300);
        return new JobParametersBuilder()
                .addLocalDate("businessDate", LocalDate.now())
                .addString("inputFile", "classpath:data/test-order-line-items.csv")
                .toJobParameters();
    }
}
