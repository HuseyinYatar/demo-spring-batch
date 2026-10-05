package com.batch.demo.batch.step3;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.data.domain.PageRequest;

import com.batch.demo.config.BatchProperties;
import com.batch.demo.domain.DailySalesReport;
import com.batch.demo.repository.CustomerSpend;
import com.batch.demo.repository.DailySalesAggregate;
import com.batch.demo.repository.DailySalesReportRepository;
import com.batch.demo.repository.InvoiceRepository;

/**
 * Computes the day's aggregate totals and top customers via repository queries (no
 * chunk-oriented reading needed for a single summary row), upserts one
 * {@link DailySalesReport} row, and writes a top-customers CSV. Mirrors
 * InvoiceSummaryMergeTasklet's plain-Tasklet, manual-I/O style - a one-shot summary
 * doesn't need chunk semantics.
 */
public class DailySalesSummaryTasklet implements Tasklet {

    private static final String TOP_CUSTOMERS_HEADER = "customerName,totalSpend";

    private final InvoiceRepository invoiceRepository;
    private final DailySalesReportRepository dailySalesReportRepository;
    private final BatchProperties properties;

    public DailySalesSummaryTasklet(InvoiceRepository invoiceRepository,
                                     DailySalesReportRepository dailySalesReportRepository,
                                     BatchProperties properties) {
        this.invoiceRepository = invoiceRepository;
        this.dailySalesReportRepository = dailySalesReportRepository;
        this.properties = properties;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        LocalDate businessDate = chunkContext.getStepContext().getStepExecution()
                .getJobParameters().getLocalDate("businessDate");

        DailySalesAggregate aggregate = invoiceRepository.aggregateByIssuedDate(businessDate);
        List<CustomerSpend> topCustomers = invoiceRepository.findTopCustomersByIssuedDate(
                businessDate, PageRequest.of(0, properties.getReportTopCustomerCount()));

        // Upserted rather than a bare save(new DailySalesReport(...)) so a re-run of
        // this tasklet for an already-reported date is idempotent instead of tripping
        // the business_date unique constraint.
        DailySalesReport report = dailySalesReportRepository.findByBusinessDate(businessDate)
                .orElseGet(() -> DailySalesReport.builder().businessDate(businessDate).build());
        report.setInvoiceCount(aggregate.getInvoiceCount());
        report.setTotalSubtotal(aggregate.getTotalSubtotal());
        report.setTotalTax(aggregate.getTotalTax());
        report.setTotalAmount(aggregate.getTotalAmount());
        // Assigned even when there is no top customer: on a re-run for an already-reported
        // date the row still holds the previous run's values, which would otherwise
        // survive next to an invoiceCount of 0.
        CustomerSpend topCustomer = topCustomers.isEmpty() ? null : topCustomers.get(0);
        report.setTopCustomerName(topCustomer != null ? topCustomer.getCustomerName() : null);
        report.setTopCustomerTotal(topCustomer != null ? topCustomer.getTotalSpend() : null);
        report.setGeneratedAt(Instant.now());
        dailySalesReportRepository.save(report);

        writeTopCustomersCsv(businessDate, topCustomers);

        return RepeatStatus.FINISHED;
    }

    private void writeTopCustomersCsv(LocalDate businessDate, List<CustomerSpend> topCustomers) {
        DailySalesReportPaths.ensureDirectoryExists(properties.getDailySalesReportOutputDir());
        Path path = DailySalesReportPaths.topCustomersCsvPath(properties.getDailySalesReportOutputDir(), businessDate);
        try (BufferedWriter writer = Files.newBufferedWriter(path,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            writer.write(TOP_CUSTOMERS_HEADER);
            writer.newLine();
            for (CustomerSpend customer : topCustomers) {
                writer.write(customer.getCustomerName() + "," + customer.getTotalSpend());
                writer.newLine();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to write top customers CSV to " + path, e);
        }
    }
}
