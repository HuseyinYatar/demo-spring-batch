package com.batch.demo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

import com.batch.demo.batch.step2.InvoiceSummaryPartitionPaths;
import com.batch.demo.config.BatchProperties;
import com.batch.demo.repository.OrderLineItemStagingRepository;
import com.batch.demo.repository.OrderRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;
import com.batch.demo.testsupport.StepExecutions;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every other test forces batch.partition-grid-size=1 (see application-test.properties)
 * for deterministic assertions - real multi-thread partitioning (both steps run
 * gridSize=6 in production) is otherwise completely untested. This class raises it to
 * 3 to verify the partitioned pipeline is actually correct under real parallelism, not
 * just "doesn't crash": every partition does its share of the work, nothing is
 * duplicated or dropped across partition boundaries, and the per-partition
 * invoice-summary files get merged and cleaned up correctly.
 */
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = {
        "batch.input-csv-path=classpath:data/test-order-line-items-partitioned.csv",
        "batch.partition-grid-size=3"
})
@SpringBatchTest
class PartitionedProcessingTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderLineItemStagingRepository stagingRepository;

    @Autowired
    private BatchProperties batchProperties;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void allPartitionsCompleteAndMergeCorrectly() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("businessDate", LocalDate.now())
                .addString("inputFile", "classpath:data/test-order-line-items-partitioned.csv")
                .toJobParameters();

        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        // 10 CSV rows split across 3 real partitions - confirm all 3 actually ran and
        // together accounted for every row, none dropped or double-counted.
        List<StepExecution> ingestPartitions = StepExecutions.matchingPrefix(execution, "ingestLineItemsWorkerStep");
        assertThat(ingestPartitions).hasSize(3);
        assertThat(ingestPartitions).allSatisfy(step -> assertThat(step.getStatus()).isEqualTo(BatchStatus.COMPLETED));
        assertThat(ingestPartitions.stream().mapToLong(StepExecution::getReadCount).sum()).isEqualTo(10);
        assertThat(ingestPartitions.stream().mapToLong(StepExecution::getWriteCount).sum()).isEqualTo(10);

        // 9 distinct orders split across 3 real partitions by sorted order-id range.
        List<StepExecution> buildPartitions = StepExecutions.matchingPrefix(execution, "buildInvoicesWorkerStep");
        assertThat(buildPartitions).hasSize(3);
        assertThat(buildPartitions).allSatisfy(step -> assertThat(step.getStatus()).isEqualTo(BatchStatus.COMPLETED));
        assertThat(buildPartitions.stream().mapToLong(StepExecution::getReadCount).sum()).isEqualTo(9);
        assertThat(buildPartitions.stream().mapToLong(StepExecution::getWriteCount).sum()).isEqualTo(9);

        assertThat(stagingRepository.count()).isEqualTo(10);
        assertThat(stagingRepository.findAll()).allSatisfy(row -> assertThat(row.isProcessed()).isTrue());
        assertThat(orderRepository.count()).isEqualTo(9);
        assertThat(orderRepository.findAll()).allSatisfy(order -> assertThat(order.getInvoice()).isNotNull());

        Long lineItemCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM order_line_item", Long.class);
        assertThat(lineItemCount).isEqualTo(10);

        // mergeInvoiceSummaryStep must have recombined all 3 partition files into one
        // and deleted the partition files - not left them lying around.
        assertThat(InvoiceSummaryPartitionPaths.listPartitionFiles(batchProperties.getInvoiceSummaryOutputPath()))
                .isEmpty();

        Path merged = Path.of(batchProperties.getInvoiceSummaryOutputPath());
        assertThat(Files.exists(merged)).isTrue();
        List<String> mergedLines = readLines(merged);
        assertThat(mergedLines.get(0))
                .isEqualTo("orderNumber,customerName,invoiceNumber,subtotal,taxAmount,totalAmount,issuedDate");
        assertThat(mergedLines).hasSize(10); // header + 9 orders
    }

    private List<String> readLines(Path path) throws IOException {
        return Files.readAllLines(path);
    }
}
