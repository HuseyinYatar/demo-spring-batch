package com.batch.demo.batch.step2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;

import com.batch.demo.config.BatchProperties;

/**
 * Recombines the per-partition invoice-summary files written by
 * buildInvoicesStep (see BuildInvoicesStepConfig) into the single
 * invoice-summary.csv the job is documented to produce, then removes the
 * partition files. Runs as a plain Tasklet - a one-shot file merge, not a
 * chunk-oriented read/process/write.
 */
public class InvoiceSummaryMergeTasklet implements Tasklet {

    private static final String HEADER = "orderNumber,customerName,invoiceNumber,subtotal,taxAmount,totalAmount,issuedDate";

    private final BatchProperties properties;

    public InvoiceSummaryMergeTasklet(BatchProperties properties) {
        this.properties = properties;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        String basePath = properties.getInvoiceSummaryOutputPath();
        List<Path> partitionFiles = InvoiceSummaryPartitionPaths.listPartitionFiles(basePath);

        Path merged = Path.of(basePath);
        try {
            try (BufferedWriter writer = Files.newBufferedWriter(merged,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                writer.write(HEADER);
                writer.newLine();
                for (Path partitionFile : partitionFiles) {
                    for (String line : Files.readAllLines(partitionFile)) {
                        if (!line.equals(HEADER)) {
                            writer.write(line);
                            writer.newLine();
                        }
                    }
                }
            }
            for (Path partitionFile : partitionFiles) {
                Files.deleteIfExists(partitionFile);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to merge invoice summary partition files into " + merged, e);
        }

        return RepeatStatus.FINISHED;
    }
}
