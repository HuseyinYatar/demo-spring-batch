package com.batch.demo;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import com.batch.demo.batch.step2.InvoiceSummaryPartitionPaths;
import com.batch.demo.batch.support.BusinessDatePaths;
import com.batch.demo.config.BatchProperties;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Output files are suffixed with the run's businessDate, so a run for one date must not
 * overwrite, truncate or merge into another date's rejected-rows / invoice-summary files.
 *
 * Uses the rejects CSV so both file kinds have content to lose: 2 READ + 2 PROCESS
 * rejects and 3 invoices on the first run. The second run (another date, same input)
 * re-skips the same 4 rows, but finds every order already staged and invoiced, so its
 * summary has no invoice rows - which makes the two dates' files distinguishable.
 */
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = "batch.input-csv-path=classpath:data/test-order-line-items-with-rejects.csv")
@SpringBatchTest
class BusinessDateOutputIsolationTest extends AbstractPostgresIntegrationTest {

    private static final String INPUT_FILE = "classpath:data/test-order-line-items-with-rejects.csv";
    private static final String SUMMARY_HEADER =
            "orderNumber,customerName,invoiceNumber,subtotal,taxAmount,totalAmount,issuedDate";

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private BatchProperties batchProperties;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void aSecondBusinessDateDoesNotOverwriteTheFirstDatesOutputFiles() throws Exception {
        LocalDate firstDate = LocalDate.of(2099, 3, 1);
        LocalDate secondDate = LocalDate.of(2099, 3, 2);

        assertThat(runJobFor(firstDate)).isEqualTo(BatchStatus.COMPLETED);

        Path firstRejects = rejectsPath(firstDate);
        Path firstSummary = summaryPath(firstDate);
        List<String> firstRejectLines = Files.readAllLines(firstRejects);
        List<String> firstSummaryLines = Files.readAllLines(firstSummary);
        // Sanity-check the baseline, so the "unchanged" assertions below can't pass on empty files.
        assertThat(rejectLines(firstRejectLines, "READ")).hasSize(2);
        assertThat(rejectLines(firstRejectLines, "PROCESS")).hasSize(2);
        assertThat(firstSummaryLines.get(0)).isEqualTo(SUMMARY_HEADER);
        assertThat(firstSummaryLines).hasSize(4); // header + ORD-R01..R03

        assertThat(runJobFor(secondDate)).isEqualTo(BatchStatus.COMPLETED);

        assertThat(Files.readAllLines(firstRejects)).isEqualTo(firstRejectLines);
        assertThat(Files.readAllLines(firstSummary)).isEqualTo(firstSummaryLines);

        Path secondRejects = rejectsPath(secondDate);
        Path secondSummary = summaryPath(secondDate);
        assertThat(secondRejects).isNotEqualTo(firstRejects).exists();
        assertThat(secondSummary).isNotEqualTo(firstSummary).exists();

        List<String> secondRejectLines = Files.readAllLines(secondRejects);
        assertThat(rejectLines(secondRejectLines, "READ")).hasSize(2);
        assertThat(rejectLines(secondRejectLines, "PROCESS")).hasSize(2);
        assertThat(Files.readAllLines(secondSummary)).containsExactly(SUMMARY_HEADER);
    }

    /**
     * perRunStateResetListener deletes leftover partition files for a fresh JobInstance, so
     * they can't be merged into the new run's summary. That cleanup must only look at the
     * run's own date: another date's partition files (e.g. a run of that date that is
     * still in progress or awaiting a restart) must survive.
     */
    @Test
    void stalePartitionFileCleanupOnlyTouchesTheRunsOwnDate() throws Exception {
        LocalDate runDate = LocalDate.of(2099, 4, 1);
        LocalDate otherDate = LocalDate.of(2099, 4, 2);

        // partition5 simulates a leftover from an earlier run with a larger grid size
        // (this test's grid size is 1, so the run itself only writes partition0).
        Path staleOwnPartition0 = InvoiceSummaryPartitionPaths.partitionPath(summaryPath(runDate).toString(), "partition0");
        Path staleOwnPartition5 = InvoiceSummaryPartitionPaths.partitionPath(summaryPath(runDate).toString(), "partition5");
        Path otherDatePartition0 = InvoiceSummaryPartitionPaths.partitionPath(summaryPath(otherDate).toString(), "partition0");
        List<String> staleOwnLines = List.of(SUMMARY_HEADER, "STALE-OWN,x,INV-X,1.00,0.18,1.18,2099-04-01");
        List<String> otherDateLines = List.of(SUMMARY_HEADER, "OTHER-DATE,x,INV-Y,1.00,0.18,1.18,2099-04-02");
        Files.write(staleOwnPartition0, staleOwnLines);
        Files.write(staleOwnPartition5, staleOwnLines);
        Files.write(otherDatePartition0, otherDateLines);

        try {
            assertThat(runJobFor(runDate)).isEqualTo(BatchStatus.COMPLETED);

            assertThat(staleOwnPartition5).doesNotExist();
            assertThat(InvoiceSummaryPartitionPaths.listPartitionFiles(summaryPath(runDate).toString())).isEmpty();
            assertThat(Files.readAllLines(summaryPath(runDate)))
                    .hasSize(4) // header + ORD-R01..R03 only
                    .noneMatch(line -> line.startsWith("STALE-OWN") || line.startsWith("OTHER-DATE"));

            assertThat(otherDatePartition0).exists();
            assertThat(Files.readAllLines(otherDatePartition0)).isEqualTo(otherDateLines);
        } finally {
            Files.deleteIfExists(otherDatePartition0);
        }
    }

    private BatchStatus runJobFor(LocalDate businessDate) throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("businessDate", businessDate)
                .addString("inputFile", INPUT_FILE)
                .toJobParameters();
        return jobLauncherTestUtils.launchJob(jobParameters).getStatus();
    }

    private Path rejectsPath(LocalDate businessDate) {
        return Path.of(BusinessDatePaths.withBusinessDate(batchProperties.getRejectsFilePath(), businessDate));
    }

    private Path summaryPath(LocalDate businessDate) {
        return Path.of(BusinessDatePaths.withBusinessDate(batchProperties.getInvoiceSummaryOutputPath(), businessDate));
    }

    private static List<String> rejectLines(List<String> lines, String phase) {
        return lines.stream().filter(line -> line.startsWith("\"" + phase + "\"")).toList();
    }
}
