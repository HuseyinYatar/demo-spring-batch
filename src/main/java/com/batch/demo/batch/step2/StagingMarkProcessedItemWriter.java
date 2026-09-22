package com.batch.demo.batch.step2;

import java.util.List;

import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.stereotype.Component;

import com.batch.demo.batch.dto.OrderInvoiceResult;
import com.batch.demo.domain.OrderLineItemStaging;
import com.batch.demo.repository.OrderLineItemStagingRepository;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class StagingMarkProcessedItemWriter implements ItemWriter<OrderInvoiceResult> {

    private final OrderLineItemStagingRepository stagingRepository;

    @Override
    public void write(Chunk<? extends OrderInvoiceResult> chunk) {
        List<OrderLineItemStaging> stagingLines = chunk.getItems().stream()
                .flatMap(result -> result.sourceStagingLines().stream())
                .peek(staging -> staging.setProcessed(true))
                .toList();
        stagingRepository.saveAll(stagingLines);
    }
}
