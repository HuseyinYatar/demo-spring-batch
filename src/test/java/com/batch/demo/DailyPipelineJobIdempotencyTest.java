package com.batch.demo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Mirrors OrderProcessingJobIdempotencyTest for the orchestrating job, plus one extra
 * case: mixing the two endpoints for the same businessDate does NOT 409 the way two
 * calls to the same endpoint would - see the javadoc on
 * BatchJobController.launchDailyPipeline for why (confirmed via the actual stack
 * trace: AbstractStep.execute() swallows the nested JobInstanceAlreadyCompleteException
 * into a failed StepExecution rather than propagating it).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@ActiveProfiles("test")
@SpringBatchTest
class DailyPipelineJobIdempotencyTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void secondLaunchSameBusinessDateIsRejectedWithConflict() throws Exception {
        mockMvc.perform(post("/api/batch/jobs/daily-pipeline"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"));

        mockMvc.perform(post("/api/batch/jobs/daily-pipeline"))
                .andExpect(status().isConflict());
    }

    @Test
    void explicitBusinessDateOverrideAvoidsTheSameDayConflict() throws Exception {
        mockMvc.perform(post("/api/batch/jobs/daily-pipeline"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"));

        mockMvc.perform(post("/api/batch/jobs/daily-pipeline").param("businessDate", "2099-01-01"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"));
    }

    /**
     * The orchestrator's own JobInstance never ran for this businessDate before, so
     * launching it succeeds at the outer jobLauncher.run(...) call (HTTP 200) - but
     * orderProcessingJobStep's nested jobOperator.start(orderProcessingJob, ...) then
     * hits JobInstanceAlreadyCompleteException against the already-completed standalone
     * run, which AbstractStep.execute() catches and records as a failed StepExecution
     * rather than propagating. Net result: 200 with body status FAILED, not 409 - a
     * real trap worth a dedicated assertion rather than leaving it to be discovered by
     * accident.
     */
    @Test
    void mixingStandaloneAndPipelineEndpointsForTheSameDateFailsRatherThanConflicts() throws Exception {
        mockMvc.perform(post("/api/batch/jobs/order-processing"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"));

        mockMvc.perform(post("/api/batch/jobs/daily-pipeline"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("FAILED"));
    }
}
