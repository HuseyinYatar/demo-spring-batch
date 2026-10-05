package com.batch.demo;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import com.batch.demo.batch.step3.DailySalesReportPaths;
import com.batch.demo.config.BatchProperties;
import com.batch.demo.domain.DailySalesReport;
import com.batch.demo.repository.DailySalesReportRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Edge cases of dailySalesReportJob that DailyPipelineJobTest's happy path (6 invoices,
 * one fresh report row) never reaches: a date with no invoices at all, and re-running
 * the summary tasklet against a report row that already exists (its upsert path).
 *
 * dailySalesReportJob is launched directly here rather than through /daily-pipeline:
 * the pipeline can't re-run the report for a date whose JobInstance already completed,
 * and a pipeline run over an input with no valid rows would mix this job's behaviour
 * with the order job's. Each run gets its own identifying {@code runId} so a second
 * launch for the same businessDate is a fresh JobInstance (the steps only ever read
 * businessDate).
 */
@SpringBootTest
@ActiveProfiles("test")
class DailySalesReportEdgeCasesTest extends AbstractPostgresIntegrationTest {

    private static final String ORDER_INPUT = "classpath:data/test-order-line-items.csv";

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("orderProcessingJob")
    private Job orderProcessingJob;

    @Autowired
    @Qualifier("dailySalesReportJob")
    private Job dailySalesReportJob;

    @Autowired
    private DailySalesReportRepository dailySalesReportRepository;

    @Autowired
    private BatchProperties properties;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private long runCounter;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void aDateWithNoInvoicesProducesAZeroedReportAndHeaderOnlyCsvs() throws Exception {
        LocalDate date = LocalDate.of(2099, 2, 1);

        assertThat(runReport(date).getStatus()).isEqualTo(BatchStatus.COMPLETED);

        DailySalesReport report = dailySalesReportRepository.findByBusinessDate(date).orElseThrow();
        assertThat(report.getInvoiceCount()).isZero();
        assertThat(report.getTotalSubtotal()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(report.getTotalTax()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(report.getTotalAmount()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(report.getTopCustomerName()).isNull();
        assertThat(report.getTopCustomerTotal()).isNull();
        assertThat(report.getGeneratedAt()).isNotNull();

        assertThat(Files.readAllLines(DailySalesReportPaths.detailCsvPath(properties.getDailySalesReportOutputDir(), date)))
                .containsExactly("orderNumber,customerName,invoiceNumber,subtotal,taxAmount,totalAmount,issuedDate");
        assertThat(Files.readAllLines(DailySalesReportPaths.topCustomersCsvPath(properties.getDailySalesReportOutputDir(), date)))
                .containsExactly("customerName,totalSpend");
    }

    @Test
    void rerunningForAnAlreadyReportedDateUpdatesTheExistingRowInPlace() throws Exception {
        LocalDate date = LocalDate.of(2099, 2, 2);

        // First report: nothing invoiced for the date yet, so a zeroed row is created.
        assertThat(runReport(date).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        DailySalesReport first = dailySalesReportRepository.findByBusinessDate(date).orElseThrow();
        assertThat(first.getInvoiceCount()).isZero();

        // Invoices for that date arrive afterwards (invoices are stamped with the run's
        // businessDate), then the report is generated again for the same date.
        assertThat(runOrderProcessing(date).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(runReport(date).getStatus()).isEqualTo(BatchStatus.COMPLETED);

        List<DailySalesReport> rows = dailySalesReportRepository.findAll();
        assertThat(rows).hasSize(1);
        DailySalesReport second = rows.get(0);
        assertThat(second.getId()).isEqualTo(first.getId());
        assertThat(second.getInvoiceCount()).isEqualTo(6);
        assertThat(second.getTotalSubtotal()).isEqualByComparingTo(new BigDecimal("502.90"));
        assertThat(second.getTotalTax()).isEqualByComparingTo(new BigDecimal("90.52"));
        assertThat(second.getTotalAmount()).isEqualByComparingTo(new BigDecimal("593.42"));
        assertThat(second.getTopCustomerName()).isEqualTo("Test Customer 3");
        assertThat(second.getTopCustomerTotal()).isEqualByComparingTo(new BigDecimal("259.60"));
        assertThat(second.getGeneratedAt()).isAfterOrEqualTo(first.getGeneratedAt());
    }

    @Test
    void rerunAfterTheDaysInvoicesAreGoneClearsTheStaleTopCustomer() throws Exception {
        LocalDate date = LocalDate.of(2099, 2, 3);
        assertThat(runOrderProcessing(date).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(runReport(date).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(dailySalesReportRepository.findByBusinessDate(date).orElseThrow().getTopCustomerName())
                .isEqualTo("Test Customer 3");

        jdbcTemplate.execute("DELETE FROM invoice");
        assertThat(runReport(date).getStatus()).isEqualTo(BatchStatus.COMPLETED);

        DailySalesReport report = dailySalesReportRepository.findByBusinessDate(date).orElseThrow();
        assertThat(report.getInvoiceCount()).isZero();
        assertThat(report.getTotalAmount()).isEqualByComparingTo(BigDecimal.ZERO);
        // A report that says "0 invoices, top customer Test Customer 3" contradicts itself.
        assertThat(report.getTopCustomerName()).isNull();
        assertThat(report.getTopCustomerTotal()).isNull();
    }

    private JobExecution runReport(LocalDate businessDate) throws Exception {
        JobParameters parameters = new JobParametersBuilder()
                .addLocalDate("businessDate", businessDate)
                .addLong("runId", ++runCounter)
                .toJobParameters();
        return jobLauncher.run(dailySalesReportJob, parameters);
    }

    private JobExecution runOrderProcessing(LocalDate businessDate) throws Exception {
        JobParameters parameters = new JobParametersBuilder()
                .addLocalDate("businessDate", businessDate)
                .addString("inputFile", ORDER_INPUT)
                .toJobParameters();
        return jobLauncher.run(orderProcessingJob, parameters);
    }
}
