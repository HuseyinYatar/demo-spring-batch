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
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import com.batch.demo.domain.Order;
import com.batch.demo.repository.OrderLineItemStagingRepository;
import com.batch.demo.repository.OrderRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;
import com.batch.demo.testsupport.StepExecutions;
import com.batch.demo.testsupport.fault.OrderConnectionFaultInjectionTestConfig;
import com.batch.demo.testsupport.fault.SqlInsertFaultTrigger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves buildInvoicesWorkerStep's TransientDataAccessException/DataAccessResourceFailureException
 * retry registration actually engages for a genuine, correctly-classified DB
 * connection failure. Unlike ingestLineItemsStep's orderLineItemStagingWriter (a raw
 * JpaItemWriter - see IngestLineItemsDbConnectionRetryTest), OrderPersistenceItemWriter
 * writes through orderRepository.saveAll(), a real Spring Data JpaRepository call, which
 * does go through Spring's persistence exception translation - confirmed here, not
 * assumed, since the two writers behave differently and one initial attempt at this
 * (registering the same types on ingestLineItemsStep) turned out to be silently
 * ineffective.
 */
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = "batch.simulate-transient-write-failures=false")
@Import(OrderConnectionFaultInjectionTestConfig.class)
@SpringBatchTest
class BuildInvoicesDbConnectionRetryTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private SqlInsertFaultTrigger trigger;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderLineItemStagingRepository stagingRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
        trigger.disarm();
    }

    @Test
    void connectionFailureDuringInvoiceWriteRecoversViaRetry() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("businessDate", LocalDate.now())
                .addString("inputFile", "classpath:data/test-order-line-items.csv")
                .toJobParameters();

        // chunk-size=2: orders 1-2 commit, order 3's insert throws the injected
        // SQLTransientConnectionException, classified as DataAccessResourceFailureException
        // via Spring Data's repository exception translation - now retryable, so the
        // retried attempt (trigger already fired once) succeeds.
        trigger.arm(2);
        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        List<Order> orders = orderRepository.findAll();
        assertThat(orders).extracting(Order::getOrderNumber)
                .containsExactlyInAnyOrder("ORD-T01", "ORD-T02", "ORD-T03", "ORD-T04", "ORD-T05", "ORD-T06");
        assertThat(orders).allSatisfy(order -> assertThat(order.getInvoice()).isNotNull());
        assertThat(stagingRepository.count()).isEqualTo(7);
        assertThat(stagingRepository.findAll()).allSatisfy(row -> assertThat(row.isProcessed()).isTrue());

        StepExecution buildPartition = StepExecutions.named(execution, "buildInvoicesWorkerStep:partition0");
        assertThat(buildPartition.getRollbackCount()).isEqualTo(1);
        assertThat(buildPartition.getSkipCount()).isZero();
        assertThat(buildPartition.getWriteCount()).isEqualTo(6);
    }
}
