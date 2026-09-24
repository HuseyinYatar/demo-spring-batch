package com.batch.demo;

import java.time.LocalDate;
import java.util.List;

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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import com.batch.demo.domain.Order;
import com.batch.demo.repository.OrderLineItemStagingRepository;
import com.batch.demo.repository.OrderRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;
import com.batch.demo.testsupport.StepExecutions;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shared test profile disables FlakyOrderPersistenceSimulator
 * (batch.simulate-transient-write-failures=false) for every other test, since it's an
 * unrelated confound once a dedicated fault injector exists (see
 * OrderProcessingJobFailureRestartTest). This class re-enables it via
 * @TestPropertySource specifically to prove the demo's actual "retry recovers from a
 * transient failure" path - the one FlakyOrderPersistenceSimulator exists to
 * demonstrate - genuinely works end-to-end, not just via manual curl testing.
 */
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = "batch.simulate-transient-write-failures=true")
@SpringBatchTest
class BuildInvoicesRetrySuccessTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderLineItemStagingRepository stagingRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void transientWriteFailureRecoversViaRetryWithoutLosingData() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("businessDate", LocalDate.now())
                .addString("inputFile", "classpath:data/test-order-line-items.csv")
                .toJobParameters();

        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        List<Order> orders = orderRepository.findAll();
        assertThat(orders).extracting(Order::getOrderNumber)
                .containsExactlyInAnyOrder("ORD-T01", "ORD-T02", "ORD-T03", "ORD-T04", "ORD-T05", "ORD-T06");
        assertThat(orders).allSatisfy(order -> assertThat(order.getInvoice()).isNotNull());
        assertThat(stagingRepository.count()).isEqualTo(7);
        assertThat(stagingRepository.findAll()).allSatisfy(row -> assertThat(row.isProcessed()).isTrue());

        StepExecution buildPartition = StepExecutions.named(execution, "buildInvoicesWorkerStep:partition0");
        // Exactly one chunk transaction rolled back (FlakyOrderPersistenceSimulator
        // fires once per job run) and the retried attempt succeeded - proven by
        // completing with every order written and zero skips, despite the failure.
        assertThat(buildPartition.getRollbackCount()).isEqualTo(1);
        assertThat(buildPartition.getSkipCount()).isZero();
        assertThat(buildPartition.getWriteCount()).isEqualTo(6);
    }
}
