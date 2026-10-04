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
 * PartitionedProcessingTest proves the partition StepExecutions' own read/write counts
 * add up, but it sums them itself - it never goes through
 * GET /api/batch/jobs/executions/{id}, which is where JobExecutionStatusResponse.from
 * does the job-level aggregation. This class closes that gap: real gridSize=3
 * partitioning, then the status endpoint's JSON is checked against the work the CSV
 * actually contains (10 rows ingested + 9 orders invoiced), not against the step data
 * the endpoint was itself derived from.
 *
 * The expected totals are hard-coded on purpose: a manager StepExecution already
 * carries the sum of its workers' counts, so an endpoint that naively adds up every
 * StepExecution on the job would double-count each partitioned step.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@ActiveProfiles("test")
@TestPropertySource(properties = {
        "batch.input-csv-path=classpath:data/test-order-line-items-partitioned.csv",
        "batch.partition-grid-size=3"
})
class PartitionedJobStatusEndpointTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void statusEndpointReportsJobLevelCountsAcrossAllPartitions() throws Exception {
        String launchBody = mockMvc.perform(post("/api/batch/jobs/order-processing").param("businessDate", "2099-01-01"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andReturn().getResponse().getContentAsString();
        long executionId = ((Number) JsonPath.read(launchBody, "$.jobExecutionId")).longValue();

        // 10 CSV rows read and staged + 9 distinct orders read and invoiced, no skips.
        mockMvc.perform(get("/api/batch/jobs/executions/{id}", executionId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobExecutionId").value(executionId))
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.readCount").value(19))
                .andExpect(jsonPath("$.writeCount").value(19))
                .andExpect(jsonPath("$.skipCount").value(0));
    }
}
