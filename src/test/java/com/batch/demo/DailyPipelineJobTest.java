package com.batch.demo;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import com.batch.demo.batch.step3.DailySalesReportPaths;
import com.batch.demo.config.BatchProperties;
import com.batch.demo.domain.DailySalesReport;
import com.batch.demo.repository.DailySalesReportRepository;
import com.batch.demo.repository.OrderRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end happy path for dailyPipelineJob: both nested jobs (orderProcessingJob,
 * dailySalesReportJob - see DailyPipelineJobConfig) run to completion against the
 * known test-order-line-items.csv fixture (6 orders, 7 line items - see
 * OrderProcessingJobIdempotencyTest for the same counts), and the report reflects
 * exactly that data.
 *
 * No businessDate override is used here deliberately: InvoiceCalculator stamps
 * Invoice.issuedDate with LocalDate.now(), not the businessDate JobParameter (a
 * pre-existing characteristic of InvoiceCalculator, unrelated to this feature - see
 * CLAUDE.md), so dailySalesReportJob's issuedDate filter only lines up with real data
 * for a same-day run.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@ActiveProfiles("test")
@SpringBatchTest
class DailyPipelineJobTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private DailySalesReportRepository dailySalesReportRepository;

    @Autowired
    private BatchProperties properties;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void runsBothNestedJobsAndProducesACorrectReport() throws Exception {
        LocalDate today = LocalDate.now();

        mockMvc.perform(post("/api/batch/jobs/daily-pipeline"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"));

        assertThat(orderRepository.count()).isEqualTo(6);

        // The orchestrator's own execution has no step-level read/write counts of its
        // own - only its two JobStep StepExecutions - but the nested jobs' own
        // JobExecutions (looked up independently below) prove the real work happened.
        List<org.springframework.batch.core.job.JobInstance> pipelineInstances =
                jobExplorer.getJobInstances("dailyPipelineJob", 0, 10);
        assertThat(pipelineInstances).hasSize(1);
        List<JobExecution> pipelineExecutions = jobExplorer.getJobExecutions(pipelineInstances.get(0));
        assertThat(pipelineExecutions).hasSize(1);
        assertThat(pipelineExecutions.get(0).getStepExecutions()).hasSize(2);

        List<org.springframework.batch.core.job.JobInstance> orderProcessingInstances =
                jobExplorer.getJobInstances("orderProcessingJob", 0, 10);
        assertThat(orderProcessingInstances).hasSize(1);
        assertThat(jobExplorer.getJobExecutions(orderProcessingInstances.get(0)))
                .hasSize(1)
                .allSatisfy(execution -> assertThat(execution.getStatus().name()).isEqualTo("COMPLETED"));

        List<org.springframework.batch.core.job.JobInstance> reportInstances =
                jobExplorer.getJobInstances("dailySalesReportJob", 0, 10);
        assertThat(reportInstances).hasSize(1);
        assertThat(jobExplorer.getJobExecutions(reportInstances.get(0)))
                .hasSize(1)
                .allSatisfy(execution -> assertThat(execution.getStatus().name()).isEqualTo("COMPLETED"));

        Optional<DailySalesReport> reportOpt = dailySalesReportRepository.findByBusinessDate(today);
        assertThat(reportOpt).isPresent();
        DailySalesReport report = reportOpt.get();
        assertThat(report.getInvoiceCount()).isEqualTo(6);
        assertThat(report.getTotalSubtotal()).isEqualByComparingTo(new BigDecimal("502.90"));
        assertThat(report.getTotalTax()).isEqualByComparingTo(new BigDecimal("90.52"));
        assertThat(report.getTotalAmount()).isEqualByComparingTo(new BigDecimal("593.42"));
        assertThat(report.getTopCustomerName()).isEqualTo("Test Customer 3");
        assertThat(report.getTopCustomerTotal()).isEqualByComparingTo(new BigDecimal("259.60"));

        assertThat(Files.exists(DailySalesReportPaths.detailCsvPath(properties.getDailySalesReportOutputDir(), today)))
                .isTrue();
        assertThat(Files.exists(DailySalesReportPaths.topCustomersCsvPath(properties.getDailySalesReportOutputDir(), today)))
                .isTrue();
    }
}
