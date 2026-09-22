package com.batch.demo.batch.step2;

import java.util.List;

import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.stereotype.Component;

import com.batch.demo.batch.dto.OrderInvoiceResult;
import com.batch.demo.domain.Order;
import com.batch.demo.repository.OrderRepository;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OrderPersistenceItemWriter implements ItemWriter<OrderInvoiceResult> {

    private final OrderRepository orderRepository;
    private final FlakyOrderPersistenceSimulator flakySimulator;

    @Override
    public void write(Chunk<? extends OrderInvoiceResult> chunk) {
        flakySimulator.maybeFailOnce();

        List<Order> orders = chunk.getItems().stream()
                .map(OrderInvoiceResult::order)
                .toList();
        orderRepository.saveAll(orders);
    }
}
