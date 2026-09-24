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
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import com.batch.demo.repository.OrderLineItemStagingRepository;
import com.batch.demo.repository.OrderRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;
import com.batch.demo.testsupport.StepExecutions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@ActiveProfiles("test")
@SpringBatchTest
class OrderProcessingJobIdempotencyTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

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
    void secondLaunchSameBusinessDateIsRejectedWithConflict() throws Exception {
        mockMvc.perform(post("/api/batch/jobs/order-processing"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"));

        mockMvc.perform(post("/api/batch/jobs/order-processing"))
                .andExpect(status().isConflict());

        assertThat(orderRepository.count()).isEqualTo(6);
        assertThat(stagingRepository.count()).isEqualTo(7);
        assertThat(stagingRepository.findAll()).allSatisfy(row -> assertThat(row.isProcessed()).isTrue());
    }

    @Test
    void reRunWithDifferentBusinessDateDoesNotDuplicateRows() throws Exception {
        JobParameters firstRun = new JobParametersBuilder()
                .addLocalDate("businessDate", LocalDate.now())
                .addString("inputFile", "classpath:data/test-order-line-items.csv")
                .toJobParameters();
        JobExecution firstExecution = jobLauncherTestUtils.launchJob(firstRun);
        assertThat(firstExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        JobParameters secondRun = new JobParametersBuilder()
                .addLocalDate("businessDate", LocalDate.now().minusDays(1))
                .addString("inputFile", "classpath:data/test-order-line-items.csv")
                .toJobParameters();
        JobExecution secondExecution = jobLauncherTestUtils.launchJob(secondRun);
        assertThat(secondExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(orderRepository.count()).isEqualTo(6);
        assertThat(stagingRepository.count()).isEqualTo(7);
        assertThat(stagingRepository.findAll()).allSatisfy(row -> assertThat(row.isProcessed()).isTrue());

        StepExecution secondIngest = StepExecutions.named(secondExecution, "ingestLineItemsWorkerStep:partition0");
        assertThat(secondIngest.getReadCount()).isEqualTo(7);
        assertThat(secondIngest.getWriteCount()).isZero();

        StepExecution secondBuild = StepExecutions.named(secondExecution, "buildInvoicesWorkerStep:partition0");
        assertThat(secondBuild.getReadCount()).isZero();
        assertThat(secondBuild.getWriteCount()).isZero();
    }
}
