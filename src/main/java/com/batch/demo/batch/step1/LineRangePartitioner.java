package com.batch.demo.batch.step1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import com.batch.demo.config.BatchProperties;

/**
 * Splits the CSV's data rows (everything after the header) into gridSize
 * contiguous line ranges, one per partition. Each partition's step-scoped
 * orderLineItemReader uses linesToSkip/maxItemCount to read only its own
 * range - see IngestLineItemsStepConfig.
 */
public class LineRangePartitioner implements Partitioner {

    private final Resource csvResource;

    public LineRangePartitioner(BatchProperties properties, ResourceLoader resourceLoader) {
        this.csvResource = resourceLoader.getResource(properties.getInputCsvPath());
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        int totalDataLines = countDataLines();
        int linesPerPartition = totalDataLines == 0 ? 0 : (int) Math.ceil((double) totalDataLines / gridSize);

        Map<String, ExecutionContext> partitions = new LinkedHashMap<>();
        int fromLine = 0;
        for (int partitionIndex = 0; partitionIndex < gridSize; partitionIndex++) {
            int toLine = Math.min(fromLine + linesPerPartition, totalDataLines);

            ExecutionContext context = new ExecutionContext();
            context.putInt("linesToSkip", 1 + fromLine);
            context.putInt("maxItemCount", Math.max(0, toLine - fromLine));
            context.putString("partitionKey", "partition" + partitionIndex);
            partitions.put("partition" + partitionIndex, context);

            fromLine = toLine;
        }

        return partitions;
    }

    private int countDataLines() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(csvResource.getInputStream(), StandardCharsets.UTF_8))) {
            long lineCount = reader.lines().count();
            return (int) Math.max(0, lineCount - 1);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to count lines in " + csvResource, e);
        }
    }
}
