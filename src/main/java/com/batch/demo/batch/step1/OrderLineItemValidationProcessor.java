package com.batch.demo.batch.step1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.batch.demo.batch.dto.OrderLineCsvRecord;
import com.batch.demo.batch.validation.OrderLineValidator;
import com.batch.demo.domain.OrderLineItemStaging;
import com.batch.demo.repository.OrderLineItemStagingRepository;

import lombok.RequiredArgsConstructor;

/**
 * Orchestrates "validate then map" only - rule logic lives in {@link OrderLineValidator}
 * and its rules, not here (single responsibility).
 */
@Component
@RequiredArgsConstructor
public class OrderLineItemValidationProcessor implements ItemProcessor<OrderLineCsvRecord, OrderLineItemStaging> {

    private static final Logger log = LoggerFactory.getLogger(OrderLineItemValidationProcessor.class);

    private final OrderLineValidator validator;
    private final OrderLineItemStagingRepository stagingRepository;

    @Override
    public OrderLineItemStaging process(OrderLineCsvRecord item) {
        validator.validate(item);

        if (stagingRepository.existsByOrderIdAndProductId(item.getOrderId(), item.getProductId())) {
            log.debug("Filtering already-staged line item, orderId={}, productId={}",
                    item.getOrderId(), item.getProductId());
            return null;
        }

        return OrderLineItemStaging.builder()
                .orderId(item.getOrderId())
                .customerId(item.getCustomerId())
                .customerName(item.getCustomerName())
                .productId(item.getProductId())
                .productName(item.getProductName())
                .quantity(item.getQuantity())
                .unitPrice(item.getUnitPrice())
                .orderDate(item.getOrderDate())
                .processed(false)
                .build();
    }
}
