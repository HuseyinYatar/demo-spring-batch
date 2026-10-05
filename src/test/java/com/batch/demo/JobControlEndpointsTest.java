package com.batch.demo;

import java.time.LocalDate;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import com.batch.demo.repository.OrderRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;
import com.batch.demo.testsupport.RunningJobs;
import com.batch.demo.testsupport.slow.SlowStagingInsertTrigger;
import com.batch.demo.testsupport.slow.SlowingDataSourceTestConfig;
import com.jayway.jsonpath.JsonPath;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The HTTP side of stop / restart / abandon: JobControlOperationsTest and the restart
 * tests call JobControlService directly, so nothing else exercises the routes
 * themselves or BatchOperationExceptionHandler's 404 / 409 mapping.
 *
 * Running executions are stopped the same way JobControlOperationsTest does it (a
 * background launch widened by SlowStagingInsertTrigger), except the stop request goes
 * through MockMvc.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Import(SlowingDataSourceTestConfig.class)
@SpringBatchTest
class JobControlEndpointsTest extends AbstractPostgresIntegrationTest {

    private static final String EXECUTION = "/api/batch/jobs/executions/{id}";
    private static final String STOP = EXECUTION + "/stop";
    private static final String RESTART = EXECUTION + "/restart";
    private static final String ABANDON = EXECUTION + "/abandon";

    private static final long UNKNOWN_EXECUTION_ID = 987_654_321L;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private SlowStagingInsertTrigger trigger;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
        trigger.disarm();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void statusOfAnUnknownExecutionIsNotFound() throws Exception {
        mockMvc.perform(get(EXECUTION, UNKNOWN_EXECUTION_ID))
                .andExpect(status().isNotFound());
    }

    @Test
    void stopRestartAndAbandonOfAnUnknownExecutionAreNotFound() throws Exception {
        mockMvc.perform(post(STOP, UNKNOWN_EXECUTION_ID)).andExpect(status().isNotFound());
        mockMvc.perform(post(RESTART, UNKNOWN_EXECUTION_ID)).andExpect(status().isNotFound());
        mockMvc.perform(post(ABANDON, UNKNOWN_EXECUTION_ID)).andExpect(status().isNotFound());
    }

    @Test
    void stopEndpointStopsARunningExecution() throws Exception {
        long stoppedId = launchAndStopViaEndpoint();

        assertThat(statusOf(stoppedId)).isEqualTo(BatchStatus.STOPPED);
        // ingestLineItemsStep was still mid-flight when stopped, so no Order can exist yet.
        assertThat(orderRepository.count()).isZero();
    }

    @Test
    void stoppingAnExecutionThatIsNotRunningIsRejectedWithConflict() throws Exception {
        long completedId = launchOrderProcessingToCompletion();

        mockMvc.perform(post(STOP, completedId))
                .andExpect(status().isConflict());
    }

    @Test
    void restartEndpointResumesAStoppedExecutionAsANewExecution() throws Exception {
        long stoppedId = launchAndStopViaEndpoint();
        assertThat(statusOf(stoppedId)).isEqualTo(BatchStatus.STOPPED);
        trigger.disarm();

        mockMvc.perform(post(RESTART, stoppedId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.jobName").value("orderProcessingJob"))
                .andExpect(jsonPath("$.jobExecutionId").value(not(equalTo((int) stoppedId))));

        assertThat(orderRepository.count()).isEqualTo(6);
    }

    @Test
    void restartingACompletedExecutionIsRejectedWithConflict() throws Exception {
        long completedId = launchOrderProcessingToCompletion();

        mockMvc.perform(post(RESTART, completedId))
                .andExpect(status().isConflict());
    }

    @Test
    void abandonEndpointMarksAStoppedExecutionAbandoned() throws Exception {
        long stoppedId = launchAndStopViaEndpoint();
        assertThat(statusOf(stoppedId)).isEqualTo(BatchStatus.STOPPED);

        mockMvc.perform(post(ABANDON, stoppedId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobExecutionId").value(stoppedId))
                .andExpect(jsonPath("$.status").value("ABANDONED"));

        assertThat(statusOf(stoppedId)).isEqualTo(BatchStatus.ABANDONED);
    }

    @Test
    void abandoningACompletedExecutionIsRejectedWithConflict() throws Exception {
        long completedId = launchOrderProcessingToCompletion();

        mockMvc.perform(post(ABANDON, completedId))
                .andExpect(status().isConflict());
    }

    /**
     * Returns the execution id, not the JobExecution: a JobExecution-returning method
     * declared on a @SpringBatchTest class gets invoked by JobScopeTestExecutionListener
     * before every test (see RunningJobs).
     */
    private long launchAndStopViaEndpoint() throws Exception {
        trigger.arm(300);
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("businessDate", LocalDate.now())
                .addString("inputFile", "classpath:data/test-order-line-items.csv")
                .toJobParameters();

        return RunningJobs.launchAndStop(jobLauncherTestUtils, jobExplorer, executor, jobParameters,
                executionId -> mockMvc.perform(post(STOP, executionId))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.jobExecutionId").value(executionId))
                        .andExpect(jsonPath("$.jobName").value("orderProcessingJob"))).getId();
    }

    private BatchStatus statusOf(long executionId) {
        return jobExplorer.getJobExecution(executionId).getStatus();
    }

    private long launchOrderProcessingToCompletion() throws Exception {
        String body = mockMvc.perform(post("/api/batch/jobs/order-processing"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andReturn().getResponse().getContentAsString();
        return ((Number) JsonPath.read(body, "$.jobExecutionId")).longValue();
    }
}
