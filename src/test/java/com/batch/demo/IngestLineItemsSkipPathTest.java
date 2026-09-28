package com.batch.demo;

import java.nio.file.Files;
import java.nio.file.Path;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import com.batch.demo.config.BatchProperties;
import com.batch.demo.domain.OrderLineItemStaging;
import com.batch.demo.repository.OrderLineItemStagingRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;
import com.batch.demo.testsupport.StepExecutions;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Uses its own input CSV (test-order-line-items-with-rejects.csv, via
 * @TestPropertySource) rather than the shared test-order-line-items.csv fixture, so
 * it gets its own Spring context/Testcontainers Postgres and doesn't disturb the
 * exact row-count assertions the other test classes make against the shared fixture.
 */
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = "batch.input-csv-path=classpath:data/test-order-line-items-with-rejects.csv")
@SpringBatchTest
class IngestLineItemsSkipPathTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

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
    void skipsMalformedAndInvalidRowsButProcessesValidOnes() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("businessDate", LocalDate.now())
                .addString("inputFile", "classpath:data/test-order-line-items-with-rejects.csv")
                .toJobParameters();

        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        StepExecution ingestPartition = StepExecutions.named(execution, "ingestLineItemsWorkerStep:partition0");
        // 7 lines total: 2 fail at read (bad quantity, bad date), so only 5 are
        // successfully read; of those, 2 fail validation (blank orderId; blank
        // customerName + zero quantity together), leaving 3 written.
        assertThat(ingestPartition.getReadCount()).isEqualTo(5);
        assertThat(ingestPartition.getReadSkipCount()).isEqualTo(2);
        assertThat(ingestPartition.getProcessSkipCount()).isEqualTo(2);
        assertThat(ingestPartition.getSkipCount()).isEqualTo(4);
        assertThat(ingestPartition.getWriteCount()).isEqualTo(3);

        assertThat(stagingRepository.findAll()).extracting(OrderLineItemStaging::getOrderId)
                .containsExactlyInAnyOrder("ORD-R01", "ORD-R02", "ORD-R03");

        String rejectedRowsContent = Files.readString(Path.of(batchProperties.getRejectsFilePath()));
        assertThat(rejectedRowsContent)
                .contains("orderId must not be blank")
                .contains("customerName must not be blank")
                .contains("quantity must be a positive number");

        long readSkipLines = rejectedRowsContent.lines().filter(line -> line.startsWith("\"READ\"")).count();
        long processSkipLines = rejectedRowsContent.lines().filter(line -> line.startsWith("\"PROCESS\"")).count();
        assertThat(readSkipLines).isEqualTo(2);
        assertThat(processSkipLines).isEqualTo(2);
    }
}
