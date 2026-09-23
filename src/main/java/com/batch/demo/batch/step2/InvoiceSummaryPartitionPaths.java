package com.batch.demo.batch.step2;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Shared naming/lookup for the per-partition invoice-summary files written by
 * buildInvoicesStep's partitioned run (see BuildInvoicesStepConfig) and
 * recombined by InvoiceSummaryMergeTasklet. Kept in one place so the writer
 * side and the merge side can never disagree on the naming scheme.
 */
public final class InvoiceSummaryPartitionPaths {

    private InvoiceSummaryPartitionPaths() {
    }

    public static Path partitionPath(String basePath, String partitionKey) {
        Path base = Path.of(basePath);
        String fileName = base.getFileName().toString();
        String suffix = "-" + (partitionKey == null ? "partition0" : partitionKey);
        int dot = fileName.lastIndexOf('.');
        String partitionedName = dot < 0 ? fileName + suffix : fileName.substring(0, dot) + suffix + fileName.substring(dot);
        Path parent = base.getParent();
        return parent == null ? Path.of(partitionedName) : parent.resolve(partitionedName);
    }

    public static List<Path> listPartitionFiles(String basePath) {
        Path base = Path.of(basePath).toAbsolutePath();
        Path directory = base.getParent();
        String fileName = base.getFileName().toString();
        int dot = fileName.lastIndexOf('.');
        String stem = dot < 0 ? fileName : fileName.substring(0, dot);
        String extension = dot < 0 ? "" : fileName.substring(dot);
        Pattern pattern = Pattern.compile(Pattern.quote(stem) + "-partition\\d+" + Pattern.quote(extension));

        try (Stream<Path> files = Files.list(directory)) {
            return files
                    .filter(path -> pattern.matcher(path.getFileName().toString()).matches())
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to list invoice summary partition files in " + directory, e);
        }
    }
}
