package com.batch.demo;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import com.batch.demo.batch.control.JobControlService;
import com.batch.demo.domain.DailySalesReport;
import com.batch.demo.repository.DailySalesReportRepository;
import com.batch.demo.repository.OrderRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;
import com.batch.demo.testsupport.StepExecutions;
import com.batch.demo.testsupport.fault.DailySalesReportFaultInjectionTestConfig;
import com.batch.demo.testsupport.fault.SqlInsertFaultTrigger;
import com.batch.demo.web.dto.JobExecutionStatusResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Proves the core selling point of JobStep-based composition (see
 * DailyPipelineJobConfig): restarting a failed dailyPipelineJob execution must not
 * re-invoke a nested job whose own JobStep already completed, while a nested job whose
 * JobStep failed gets a genuine restart (new JobExecution, same child JobInstance).
 *
 * The fault is injected on daily_sales_report's insert (see
 * DailySalesReportFaultInjectionTestConfig), so orderProcessingJobStep always completes
 * first before dailySalesReportJobStep fails.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Import(DailySalesReportFaultInjectionTestConfig.class)
@SpringBatchTest
class DailyPipelineJobRestartTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private JobControlService jobControlService;

    @Autowired
    private SqlInsertFaultTrigger trigger;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private DailySalesReportRepository dailySalesReportRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
        trigger.disarm();
    }

    @Test
    void restartResumesOnlyTheFailedNestedJobNotTheCompletedOne() throws Exception {
        LocalDate today = LocalDate.now();

        // Phase 1: orderProcessingJobStep completes; dailySalesReportJobStep fails on
        // its very first insert into daily_sales_report.
        trigger.arm(0);

        mockMvc.perform(post("/api/batch/jobs/daily-pipeline"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("FAILED"));

        List<JobInstance> pipelineInstances = jobExplorer.getJobInstances("dailyPipelineJob", 0, 10);
        assertThat(pipelineInstances).hasSize(1);
        List<JobExecution> pipelineExecutions = jobExplorer.getJobExecutions(pipelineInstances.get(0));
        assertThat(pipelineExecutions).hasSize(1);
        JobExecution execution = pipelineExecutions.get(0);
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);

        StepExecution orderProcessingStep = StepExecutions.named(execution, "orderProcessingJobStep");
        assertThat(orderProcessingStep.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        // getFailureExceptions() is transient, in-memory-only state on the StepExecution
        // object that actually ran a step - it never round-trips through the
        // JobRepository, so it's always empty on an instance re-fetched via JobExplorer
        // (as this one is, since dailyPipelineJob was launched via MockMvc, not
        // returned directly to this test). The exit description IS persisted
        // (BATCH_STEP_EXECUTION.EXIT_MESSAGE), so that's the field to assert against.
        StepExecution reportStep = StepExecutions.named(execution, "dailySalesReportJobStep");
        assertThat(reportStep.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(reportStep.getExitStatus().getExitDescription()).contains("delegate Job failed");

        assertThat(orderRepository.count()).isEqualTo(6);
        assertThat(dailySalesReportRepository.findByBusinessDate(today)).isEmpty();

        // The failed execution still reports the work done before the failure: 7 rows
        // ingested + 6 orders invoiced + 6 invoices read into the report's detail step,
        // which committed before the summary step hit the injected fault.
        JobExecutionStatusResponse failedStatus = JobExecutionStatusResponse.from(execution);
        assertThat(failedStatus.readCount()).isEqualTo(19);
        assertThat(failedStatus.writeCount()).isEqualTo(19);

        List<JobInstance> orderProcessingInstancesBefore = jobExplorer.getJobInstances("orderProcessingJob", 0, 10);
        assertThat(orderProcessingInstancesBefore).hasSize(1);
        assertThat(jobExplorer.getJobExecutions(orderProcessingInstancesBefore.get(0))).hasSize(1);

        // Phase 2: recover and restart via the existing, unmodified restart endpoint.
        trigger.disarm();
        JobExecutionStatusResponse restarted = jobControlService.restart(execution.getId());
        JobExecution restartedExecution = jobExplorer.getJobExecution(restarted.jobExecutionId());

        assertThat(restartedExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        // Proof orderProcessingJobStep (already COMPLETED) was never re-invoked: still
        // exactly one JobInstance/JobExecution for the nested orderProcessingJob.
        List<JobInstance> orderProcessingInstancesAfter = jobExplorer.getJobInstances("orderProcessingJob", 0, 10);
        assertThat(orderProcessingInstancesAfter).hasSize(1);
        assertThat(jobExplorer.getJobExecutions(orderProcessingInstancesAfter.get(0))).hasSize(1);
        assertThat(orderRepository.count()).isEqualTo(6);

        // Proof dailySalesReportJobStep (FAILED) got a genuine restart: same child
        // JobInstance, a second JobExecution (the failed one, then a completed one).
        List<JobInstance> reportInstances = jobExplorer.getJobInstances("dailySalesReportJob", 0, 10);
        assertThat(reportInstances).hasSize(1);
        List<JobExecution> reportExecutions = jobExplorer.getJobExecutions(reportInstances.get(0));
        assertThat(reportExecutions).hasSize(2);
        assertThat(reportExecutions).extracting(JobExecution::getStatus)
                .containsExactlyInAnyOrder(BatchStatus.FAILED, BatchStatus.COMPLETED);

        Optional<DailySalesReport> reportOpt = dailySalesReportRepository.findByBusinessDate(today);
        assertThat(reportOpt).isPresent();
        DailySalesReport report = reportOpt.get();
        assertThat(report.getInvoiceCount()).isEqualTo(6);
        assertThat(report.getTotalSubtotal()).isEqualByComparingTo(new BigDecimal("502.90"));
        assertThat(report.getTotalTax()).isEqualByComparingTo(new BigDecimal("90.52"));
        assertThat(report.getTotalAmount()).isEqualByComparingTo(new BigDecimal("593.42"));
    }
}
