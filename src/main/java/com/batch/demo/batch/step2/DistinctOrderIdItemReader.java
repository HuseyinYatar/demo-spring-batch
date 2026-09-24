package com.batch.demo.batch.step2;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemStreamException;
import org.springframework.batch.infrastructure.item.support.AbstractItemStreamItemReader;
import org.springframework.data.domain.PageRequest;

import com.batch.demo.repository.OrderLineItemStagingRepository;

/**
 * Reads distinct, not-yet-processed order ids from staging, restricted to
 * [fromOrderId, toOrderId] - the range this partition owns (see
 * OrderIdRangePartitioner). Must be step-scoped: as a singleton it would be
 * exhausted after the first job run and silently return zero items on every
 * subsequent run.
 *
 * <p>Paginated via keyset ("seek") pagination rather than loading the whole
 * range at once: each page query re-queries for ids strictly greater than the
 * last one handed out, so memory use is bounded by pageSize regardless of how
 * many unprocessed orders the partition's range contains.
 */
public class DistinctOrderIdItemReader extends AbstractItemStreamItemReader<String> implements ItemReader<String> {

    private final OrderLineItemStagingRepository stagingRepository;
    private final String fromOrderId;
    private final String toOrderId;
    private final int pageSize;

    private Iterator<String> currentPage;
    private String lastOrderId;
    private boolean noMorePages;

    public DistinctOrderIdItemReader(OrderLineItemStagingRepository stagingRepository, String fromOrderId,
            String toOrderId, int pageSize) {
        this.stagingRepository = stagingRepository;
        this.fromOrderId = fromOrderId;
        this.toOrderId = toOrderId;
        this.pageSize = pageSize;
        setName("distinctOrderIdItemReader");
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);
        lastOrderId = null;
        noMorePages = fromOrderId == null || toOrderId == null;
        currentPage = Collections.emptyIterator();
    }

    @Override
    public String read() {
        if (!currentPage.hasNext() && !noMorePages) {
            fetchNextPage();
        }
        if (!currentPage.hasNext()) {
            return null;
        }
        lastOrderId = currentPage.next();
        return lastOrderId;
    }

    private void fetchNextPage() {
        List<String> page = stagingRepository.findDistinctUnprocessedOrderIdsBetweenAfter(
                fromOrderId, toOrderId, lastOrderId, PageRequest.of(0, pageSize));
        currentPage = page.iterator();
        if (page.size() < pageSize) {
            noMorePages = true;
        }
    }

    @Override
    public void close() throws ItemStreamException {
        currentPage = null;
        lastOrderId = null;
        super.close();
    }
}
