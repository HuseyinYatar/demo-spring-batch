package com.batch.demo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;
import com.jayway.jsonpath.JsonPath;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * dailyPipelineJob's own StepExecutions are two JobSteps that process no items; the real
 * reads/writes/skips live on the nested jobs' separate JobExecutions. These tests check
 * that GET /api/batch/jobs/executions/{id} still reports the work the pipeline actually
 * did (see NestedJobCountsRollupListener), against totals worked out from the CSV
 * fixture rather than from the step data the endpoint is itself derived from.
 *
 * Uses test-order-line-items-with-rejects.csv (7 lines: 2 fail at read, 2 fail
 * validation, 3 valid single-line orders) so the skip counts are exercised too.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@ActiveProfiles("test")
@TestPropertySource(properties = "batch.input-csv-path=classpath:data/test-order-line-items-with-rejects.csv")
class DailyPipelineJobStatusEndpointTest extends AbstractPostgresIntegrationTest {

    private static final String BUSINESS_DATE = "2099-01-01";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void statusEndpointRollsUpTheNestedJobsCounts() throws Exception {
        long executionId = launch("/api/batch/jobs/daily-pipeline", "COMPLETED");

        // ingest: 5 rows read (2 more fail at read), 3 written, 4 skipped (2 read + 2 process)
        // build invoices: 3 orders read and written
        // daily sales report: 3 invoices read and written
        mockMvc.perform(get("/api/batch/jobs/executions/{id}", executionId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobName").value("dailyPipelineJob"))
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.readCount").value(11))
                .andExpect(jsonPath("$.writeCount").value(9))
                .andExpect(jsonPath("$.skipCount").value(4));
    }

    /**
     * The standalone run already COMPLETED the nested job's JobInstance, so the
     * pipeline's own start of it is rejected and no nested JobExecution is created for
     * this step. The one the explorer finds belongs to the standalone run - its counts
     * must not be borrowed for the pipeline, which did none of that work.
     */
    @Test
    void doesNotBorrowTheCountsOfAnEarlierStandaloneRunWhenTheNestedStartIsRejected() throws Exception {
        launch("/api/batch/jobs/order-processing", "COMPLETED");

        long pipelineExecutionId = launch("/api/batch/jobs/daily-pipeline", "FAILED");

        mockMvc.perform(get("/api/batch/jobs/executions/{id}", pipelineExecutionId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("FAILED"))
                .andExpect(jsonPath("$.readCount").value(0))
                .andExpect(jsonPath("$.writeCount").value(0))
                .andExpect(jsonPath("$.skipCount").value(0));
    }

    private long launch(String path, String expectedStatus) throws Exception {
        String body = mockMvc.perform(post(path).param("businessDate", BUSINESS_DATE))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(expectedStatus))
                .andReturn().getResponse().getContentAsString();
        return ((Number) JsonPath.read(body, "$.jobExecutionId")).longValue();
    }
}
