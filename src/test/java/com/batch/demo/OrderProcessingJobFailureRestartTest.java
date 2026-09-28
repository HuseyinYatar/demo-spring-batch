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

import com.batch.demo.batch.control.JobControlService;
import com.batch.demo.batch.step2.TransientInvoiceWriteException;
import com.batch.demo.domain.Order;
import com.batch.demo.domain.OrderLineItemStaging;
import com.batch.demo.repository.OrderLineItemStagingRepository;
import com.batch.demo.repository.OrderRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;
import com.batch.demo.testsupport.StepExecutions;
import com.batch.demo.testsupport.fault.OrderFaultInjectionTestConfig;
import com.batch.demo.testsupport.fault.SqlInsertFaultTrigger;
import com.batch.demo.web.dto.JobExecutionStatusResponse;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@Import(OrderFaultInjectionTestConfig.class)
@SpringBatchTest
class OrderProcessingJobFailureRestartTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private JobControlService jobControlService;

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
    void restartAfterFailureRecoversWithoutDuplicates() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("businessDate", LocalDate.now())
                .addString("inputFile", "classpath:data/test-order-line-items.csv")
                .addLong("startedAtEpochMs", System.currentTimeMillis(), false)
                .toJobParameters();

        // Phase 1: induce a genuine failure mid-way through buildInvoicesStep.
        trigger.arm(2);
        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);

        StepExecution failedPartition = StepExecutions.named(execution, "buildInvoicesWorkerStep:partition0");
        assertThat(failedPartition.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failedPartition.getFailureExceptions())
                .isNotEmpty()
                .noneMatch(TransientInvoiceWriteException.class::isInstance);

        List<Order> ordersAfterFailure = orderRepository.findAll();
        assertThat(ordersAfterFailure).extracting(Order::getOrderNumber)
                .containsExactlyInAnyOrder("ORD-T01", "ORD-T02");
        assertThat(ordersAfterFailure).allSatisfy(order -> assertThat(order.getInvoice()).isNotNull());

        assertThat(processedFlags(List.of("ORD-T01", "ORD-T02"))).containsOnly(true);
        assertThat(processedFlags(List.of("ORD-T03", "ORD-T04", "ORD-T05", "ORD-T06"))).containsOnly(false);

        // Phase 2: recover and restart - assert a clean, non-duplicating resume.
        trigger.disarm();
        JobExecutionStatusResponse restarted = jobControlService.restart(execution.getId());
        JobExecution restartedExecution = jobExplorer.getJobExecution(restarted.jobExecutionId());

        assertThat(restartedExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        List<Order> ordersAfterRestart = orderRepository.findAll();
        assertThat(ordersAfterRestart).extracting(Order::getOrderNumber)
                .containsExactlyInAnyOrder("ORD-T01", "ORD-T02", "ORD-T03", "ORD-T04", "ORD-T05", "ORD-T06");
        assertThat(ordersAfterRestart).allSatisfy(order -> assertThat(order.getInvoice()).isNotNull());
        assertThat(orderLineItemCount()).isEqualTo(7L);
        assertThat(stagingRepository.findAll()).allSatisfy(row -> assertThat(row.isProcessed()).isTrue());

        StepExecution restartedPartition = StepExecutions.named(restartedExecution, "buildInvoicesWorkerStep:partition0");
        assertThat(restartedPartition.getReadCount()).isEqualTo(4);
        assertThat(restartedPartition.getWriteCount()).isEqualTo(4);
        assertThat(restartedPartition.getFilterCount()).isZero();
    }

    private List<Boolean> processedFlags(List<String> orderIds) {
        return stagingRepository.findAll().stream()
                .filter(row -> orderIds.contains(row.getOrderId()))
                .map(OrderLineItemStaging::isProcessed)
                .toList();
    }

    private long orderLineItemCount() {
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM order_line_item", Long.class);
        return count == null ? 0 : count;
    }
}
