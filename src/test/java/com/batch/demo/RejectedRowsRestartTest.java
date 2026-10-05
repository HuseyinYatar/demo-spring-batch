package com.batch.demo;

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
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import com.batch.demo.batch.control.JobControlService;
import com.batch.demo.batch.support.BusinessDatePaths;
import com.batch.demo.config.BatchProperties;
import com.batch.demo.domain.OrderLineItemStaging;
import com.batch.demo.repository.OrderLineItemStagingRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;
import com.batch.demo.testsupport.fault.SqlInsertFaultTrigger;
import com.batch.demo.testsupport.fault.StagingFaultInjectionTestConfig;
import com.batch.demo.web.dto.JobExecutionStatusResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A restart must keep the rows the failed attempt already logged to the rejects file and
 * append its own, instead of truncating the file the way a fresh JobInstance does
 * (see the isRestart guard in BatchJobConfig.perRunStateResetListener).
 *
 * With chunk-size=2 the CSV (see test-order-line-items-restart-with-rejects.csv) chunks as:
 * <pre>
 *   chunk 1: X01 ok, X02 READ skip, X03 PROCESS skip  -> commits, 1 READ + 1 PROCESS logged
 *   chunk 2: X04, X05                                  -> injected insert fault, job FAILS
 *   chunk 3: X06 PROCESS skip, X07                     -> only reached by the restart
 * </pre>
 * so after the restart the file must hold the first attempt's two rows plus the
 * restart's one new row, under a single header.
 */
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = "batch.input-csv-path=classpath:data/test-order-line-items-restart-with-rejects.csv")
@Import(StagingFaultInjectionTestConfig.class)
@SpringBatchTest
class RejectedRowsRestartTest extends AbstractPostgresIntegrationTest {

    private static final String REJECTS_HEADER = "stage,reason,timestamp,rawContent";

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private JobControlService jobControlService;

    @Autowired
    private SqlInsertFaultTrigger trigger;

    @Autowired
    private OrderLineItemStagingRepository stagingRepository;

    @Autowired
    private BatchProperties batchProperties;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
        trigger.disarm();
    }

    @Test
    void restartKeepsTheFirstAttemptsRejectedRowsAndAppendsItsOwn() throws Exception {
        LocalDate businessDate = LocalDate.of(2099, 5, 1);
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("businessDate", businessDate)
                .addString("inputFile", "classpath:data/test-order-line-items-restart-with-rejects.csv")
                .addLong("startedAtEpochMs", System.currentTimeMillis(), false)
                .toJobParameters();
        Path rejectsFile = Path.of(BusinessDatePaths.withBusinessDate(batchProperties.getRejectsFilePath(), businessDate));

        // Phase 1: chunk 1 commits (X01 staged, X02 and X03 rejected), chunk 2's insert fails.
        trigger.arm(1);
        JobExecution failed = jobLauncherTestUtils.launchJob(jobParameters);

        assertThat(failed.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(stagingRepository.findAll()).extracting(OrderLineItemStaging::getOrderId)
                .containsExactly("ORD-X01");

        List<String> afterFailure = Files.readAllLines(rejectsFile);
        assertThat(afterFailure.get(0)).isEqualTo(REJECTS_HEADER);
        assertThat(rows(afterFailure, "READ")).singleElement().satisfies(row -> assertThat(row).contains("ORD-X02"));
        assertThat(rows(afterFailure, "PROCESS")).singleElement().satisfies(row -> assertThat(row).contains("ORD-X03"));

        // Phase 2: restart picks up after chunk 1 and hits the unit-price reject in chunk 3.
        trigger.disarm();
        JobExecutionStatusResponse restarted = jobControlService.restart(failed.getId());
        JobExecution restartedExecution = jobExplorer.getJobExecution(restarted.jobExecutionId());

        assertThat(restartedExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(stagingRepository.findAll()).extracting(OrderLineItemStaging::getOrderId)
                .containsExactlyInAnyOrder("ORD-X01", "ORD-X04", "ORD-X05", "ORD-X07");

        List<String> afterRestart = Files.readAllLines(rejectsFile);
        // Header + the first attempt's two rows + the restart's one new row, nothing re-logged.
        assertThat(afterRestart).hasSize(afterFailure.size() + 1);
        assertThat(afterRestart.stream().filter(REJECTS_HEADER::equals)).hasSize(1);
        // The first attempt's rows are still there, byte for byte and in the same order,
        // followed by the restart's own reject.
        assertThat(afterRestart.subList(0, afterFailure.size())).isEqualTo(afterFailure);
        assertThat(rows(afterRestart, "READ")).hasSize(1);
        assertThat(rows(afterRestart, "PROCESS")).hasSize(2);
        assertThat(afterRestart.get(afterRestart.size() - 1)).contains("ORD-X06");
    }

    private static List<String> rows(List<String> lines, String stage) {
        return lines.stream().filter(line -> line.startsWith("\"" + stage + "\"")).toList();
    }
}
