package com.batch.demo.batch.step2;

import java.util.Iterator;

import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemStreamException;
import org.springframework.batch.infrastructure.item.support.AbstractItemStreamItemReader;

import com.batch.demo.repository.OrderLineItemStagingRepository;

/**
 * Reads distinct, not-yet-processed order ids from staging. Must be step-scoped: as a
 * singleton it would be exhausted after the first job run and silently return zero
 * items on every subsequent run.
 */
public class DistinctOrderIdItemReader extends AbstractItemStreamItemReader<String> implements ItemReader<String> {

    private final OrderLineItemStagingRepository stagingRepository;
    private Iterator<String> orderIds;

    public DistinctOrderIdItemReader(OrderLineItemStagingRepository stagingRepository) {
        this.stagingRepository = stagingRepository;
        setName("distinctOrderIdItemReader");
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);
        orderIds = stagingRepository.findDistinctUnprocessedOrderIds().iterator();
    }

    @Override
    public String read() {
        return orderIds != null && orderIds.hasNext() ? orderIds.next() : null;
    }

    @Override
    public void close() throws ItemStreamException {
        orderIds = null;
        super.close();
    }
}
