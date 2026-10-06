package com.batch.demo.batch.step2;

import java.util.List;

import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.stereotype.Component;

import com.batch.demo.batch.dto.OrderInvoiceResult;
import com.batch.demo.domain.OrderLineItemStaging;
import com.batch.demo.repository.OrderLineItemStagingRepository;

import lombok.RequiredArgsConstructor;

/**
 * Marks a chunk's source staging rows processed with a single UPDATE by id. The rows were
 * loaded by StagedOrderItemReader in an earlier (possibly different) transaction, so they
 * are detached here: saving them would merge each one (a SELECT apiece) before updating it.
 */
@Component
@RequiredArgsConstructor
public class StagingMarkProcessedItemWriter implements ItemWriter<OrderInvoiceResult> {

    private final OrderLineItemStagingRepository stagingRepository;

    @Override
    public void write(Chunk<? extends OrderInvoiceResult> chunk) {
        List<Long> stagingIds = chunk.getItems().stream()
                .flatMap(result -> result.sourceStagingLines().stream())
                .map(OrderLineItemStaging::getId)
                .toList();
        if (!stagingIds.isEmpty()) {
            stagingRepository.markProcessed(stagingIds);
        }
    }
}
