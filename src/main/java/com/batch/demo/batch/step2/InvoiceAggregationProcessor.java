package com.batch.demo.batch.step2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.batch.demo.batch.dto.OrderInvoiceResult;
import com.batch.demo.domain.Order;
import com.batch.demo.domain.OrderLineItem;
import com.batch.demo.domain.OrderLineItemStaging;
import com.batch.demo.domain.OrderStatus;
import com.batch.demo.repository.OrderLineItemStagingRepository;
import com.batch.demo.repository.OrderRepository;

import lombok.RequiredArgsConstructor;

/**
 * For a given orderId, aggregates its staged line items into an Order + Invoice.
 * Returns null (Spring Batch filters nulls) when the order was already invoiced in a
 * prior run, or when no unprocessed staging rows remain for it - this keeps the job
 * safe to re-trigger without needing skip/exception machinery.
 */
@Component
@RequiredArgsConstructor
public class InvoiceAggregationProcessor implements ItemProcessor<String, OrderInvoiceResult> {

    private final OrderLineItemStagingRepository stagingRepository;
    private final OrderRepository orderRepository;
    private final InvoiceCalculator invoiceCalculator;

    @Override
    public OrderInvoiceResult process(String orderId) {
        if (orderRepository.existsByOrderNumber(orderId)) {
            return null;
        }

        List<OrderLineItemStaging> stagedLines = stagingRepository.findByOrderIdAndProcessedFalse(orderId);
        if (stagedLines.isEmpty()) {
            return null;
        }

        Order order = buildOrder(orderId, stagedLines);
        order.setInvoice(invoiceCalculator.calculate(order));
        order.getInvoice().setOrder(order);
        order.setStatus(OrderStatus.INVOICED);

        return new OrderInvoiceResult(order, stagedLines);
    }

    private Order buildOrder(String orderId, List<OrderLineItemStaging> stagedLines) {
        OrderLineItemStaging first = stagedLines.get(0);
        Order order = Order.builder()
                .orderNumber(orderId)
                .customerId(first.getCustomerId())
                .customerName(first.getCustomerName())
                .orderDate(first.getOrderDate())
                .status(OrderStatus.PENDING)
                .build();

        for (OrderLineItemStaging staged : stagedLines) {
            OrderLineItem lineItem = OrderLineItem.builder()
                    .productId(staged.getProductId())
                    .productName(staged.getProductName())
                    .quantity(staged.getQuantity())
                    .unitPrice(staged.getUnitPrice())
                    .lineTotal(staged.getUnitPrice()
                            .multiply(BigDecimal.valueOf(staged.getQuantity()))
                            .setScale(2, RoundingMode.HALF_UP))
                    .build();
            order.addLineItem(lineItem);
        }

        return order;
    }
}
