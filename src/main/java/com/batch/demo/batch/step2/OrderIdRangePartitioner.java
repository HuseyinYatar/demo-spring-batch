package com.batch.demo.batch.step2;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import com.batch.demo.repository.OrderLineItemStagingRepository;

/**
 * Splits the sorted distinct unprocessed order ids into gridSize contiguous,
 * non-overlapping ranges. Range-based rather than hash-based so every order is
 * guaranteed to land in exactly one partition regardless of insertion order -
 * see DistinctOrderIdItemReader. The database computes the range boundaries
 * (ntile), so the ids themselves are never loaded into memory here.
 */
public class OrderIdRangePartitioner implements Partitioner {

    private final OrderLineItemStagingRepository stagingRepository;

    public OrderIdRangePartitioner(OrderLineItemStagingRepository stagingRepository) {
        this.stagingRepository = stagingRepository;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        List<Object[]> ranges = stagingRepository.findUnprocessedOrderIdRanges(gridSize);

        Map<String, ExecutionContext> partitions = new LinkedHashMap<>();
        for (int partitionIndex = 0; partitionIndex < gridSize; partitionIndex++) {
            ExecutionContext context = new ExecutionContext();
            context.putString("partitionKey", "partition" + partitionIndex);

            if (partitionIndex < ranges.size()) {
                Object[] range = ranges.get(partitionIndex);
                context.putString("fromOrderId", (String) range[0]);
                context.putString("toOrderId", (String) range[1]);
            }

            partitions.put("partition" + partitionIndex, context);
        }

        return partitions;
    }
}
