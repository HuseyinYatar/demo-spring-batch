package com.batch.demo.batch.step2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemStreamException;
import org.springframework.batch.infrastructure.item.support.AbstractItemStreamItemReader;
import org.springframework.data.domain.PageRequest;

import com.batch.demo.batch.dto.StagedOrder;
import com.batch.demo.domain.OrderLineItemStaging;
import com.batch.demo.repository.OrderLineItemStagingRepository;
import com.batch.demo.repository.OrderRepository;

/**
 * Reads not-yet-processed orders from staging, restricted to order ids in
 * [fromOrderId, toOrderId] - the range this partition owns (see OrderIdRangePartitioner).
 * Must be step-scoped: as a singleton it would be exhausted after the first job run and
 * silently return zero items on every subsequent run.
 *
 * <p>Paginated via keyset ("seek") pagination rather than loading the whole range at
 * once: each page query re-queries for ids strictly greater than the last one of the
 * previous page, so memory use is bounded by pageSize regardless of how many unprocessed
 * orders the partition's range contains.
 *
 * <p>Each page of ids is turned into {@link StagedOrder}s with two more queries for the
 * whole page - the lines of all its orders, and which of them already have an Order -
 * instead of the two per-order lookups the processor used to run. Orders that were
 * already invoiced in a prior run, or whose lines were processed since the id page was
 * read, are dropped here and never reach the processor (they are not counted as reads).
 * The lines come back from a transaction that may already be over by the time the
 * processor and writers use them, so they must be treated as detached: nothing may rely
 * on them being managed (StagingMarkProcessedItemWriter updates by id for that reason).
 */
public class StagedOrderItemReader extends AbstractItemStreamItemReader<StagedOrder> implements ItemReader<StagedOrder> {

    private final OrderLineItemStagingRepository stagingRepository;
    private final OrderRepository orderRepository;
    private final String fromOrderId;
    private final String toOrderId;
    private final int pageSize;

    private Iterator<StagedOrder> currentPage;
    private String lastOrderId;
    private boolean noMorePages;

    public StagedOrderItemReader(OrderLineItemStagingRepository stagingRepository, OrderRepository orderRepository,
            String fromOrderId, String toOrderId, int pageSize) {
        this.stagingRepository = stagingRepository;
        this.orderRepository = orderRepository;
        this.fromOrderId = fromOrderId;
        this.toOrderId = toOrderId;
        this.pageSize = pageSize;
        setName("stagedOrderItemReader");
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);
        lastOrderId = null;
        noMorePages = fromOrderId == null || toOrderId == null;
        currentPage = Collections.emptyIterator();
    }

    @Override
    public StagedOrder read() {
        // A page can come back empty after filtering (every order already invoiced), so keep
        // paging until something is left to hand out or the range is exhausted.
        while (!currentPage.hasNext() && !noMorePages) {
            fetchNextPage();
        }
        return currentPage.hasNext() ? currentPage.next() : null;
    }

    private void fetchNextPage() {
        List<String> orderIds = stagingRepository.findDistinctUnprocessedOrderIdsBetweenAfter(
                fromOrderId, toOrderId, lastOrderId, PageRequest.of(0, pageSize));
        if (orderIds.size() < pageSize) {
            noMorePages = true;
        }
        if (orderIds.isEmpty()) {
            currentPage = Collections.emptyIterator();
            return;
        }
        // The keyset cursor follows the id page, not what survives filtering below.
        lastOrderId = orderIds.get(orderIds.size() - 1);
        currentPage = stage(orderIds).iterator();
    }

    private List<StagedOrder> stage(List<String> orderIds) {
        Set<String> alreadyInvoiced = new HashSet<>(orderRepository.findExistingOrderNumbers(orderIds));
        List<String> toInvoice = orderIds.stream().filter(id -> !alreadyInvoiced.contains(id)).toList();
        if (toInvoice.isEmpty()) {
            return List.of();
        }

        Map<String, List<OrderLineItemStaging>> linesByOrder = new LinkedHashMap<>();
        for (OrderLineItemStaging line : stagingRepository.findUnprocessedByOrderIdIn(toInvoice)) {
            linesByOrder.computeIfAbsent(line.getOrderId(), id -> new ArrayList<>()).add(line);
        }

        List<StagedOrder> staged = new ArrayList<>(toInvoice.size());
        for (String orderId : toInvoice) {
            List<OrderLineItemStaging> lines = linesByOrder.get(orderId);
            if (lines != null) {
                staged.add(new StagedOrder(orderId, lines));
            }
        }
        return staged;
    }

    @Override
    public void close() throws ItemStreamException {
        currentPage = null;
        lastOrderId = null;
        super.close();
    }
}
