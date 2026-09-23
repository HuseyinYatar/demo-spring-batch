package com.batch.demo.batch.step2;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import com.batch.demo.repository.OrderLineItemStagingRepository;

/**
 * Splits the sorted list of distinct unprocessed order ids into gridSize
 * contiguous, non-overlapping ranges. Range-based rather than hash-based so
 * every order is guaranteed to land in exactly one partition regardless of
 * insertion order - see DistinctOrderIdItemReader.
 */
public class OrderIdRangePartitioner implements Partitioner {

    private final OrderLineItemStagingRepository stagingRepository;

    public OrderIdRangePartitioner(OrderLineItemStagingRepository stagingRepository) {
        this.stagingRepository = stagingRepository;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        List<String> orderIds = stagingRepository.findDistinctUnprocessedOrderIds();
        int totalOrders = orderIds.size();
        int ordersPerPartition = totalOrders == 0 ? 0 : (int) Math.ceil((double) totalOrders / gridSize);

        Map<String, ExecutionContext> partitions = new LinkedHashMap<>();
        int fromIndex = 0;
        for (int partitionIndex = 0; partitionIndex < gridSize; partitionIndex++) {
            ExecutionContext context = new ExecutionContext();
            context.putString("partitionKey", "partition" + partitionIndex);

            if (ordersPerPartition > 0 && fromIndex < totalOrders) {
                int toIndex = Math.min(fromIndex + ordersPerPartition, totalOrders);
                context.putString("fromOrderId", orderIds.get(fromIndex));
                context.putString("toOrderId", orderIds.get(toIndex - 1));
                fromIndex = toIndex;
            }

            partitions.put("partition" + partitionIndex, context);
        }

        return partitions;
    }
}
