package com.batch.demo.batch.step3;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;

/**
 * Shared naming/lookup for dailySalesReportJob's output files, kept in one place so
 * the detail-step writer and the summary-step tasklet can never disagree on the
 * naming scheme - mirrors InvoiceSummaryPartitionPaths's role for step2.
 */
public final class DailySalesReportPaths {

    private DailySalesReportPaths() {
    }

    public static Path detailCsvPath(String baseDir, LocalDate businessDate) {
        return Path.of(baseDir, "daily-sales-detail-" + businessDate + ".csv");
    }

    public static Path topCustomersCsvPath(String baseDir, LocalDate businessDate) {
        return Path.of(baseDir, "daily-sales-top-customers-" + businessDate + ".csv");
    }

    /**
     * Unlike invoice-summary.csv (a single file in the working directory),
     * dailySalesReportOutputDir is a subdirectory - FlatFileItemWriter does not create
     * missing parent directories on its own, so callers must do this before opening a
     * writer against a path under it.
     */
    public static void ensureDirectoryExists(String baseDir) {
        try {
            Files.createDirectories(Path.of(baseDir));
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to create daily sales report output directory " + baseDir, e);
        }
    }
}
