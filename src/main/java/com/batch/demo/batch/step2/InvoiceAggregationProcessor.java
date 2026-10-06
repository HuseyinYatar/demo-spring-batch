package com.batch.demo.batch.step2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.List;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.batch.demo.batch.dto.OrderInvoiceResult;
import com.batch.demo.batch.dto.StagedOrder;
import com.batch.demo.domain.Order;
import com.batch.demo.domain.OrderLineItem;
import com.batch.demo.domain.OrderLineItemStaging;
import com.batch.demo.domain.OrderStatus;

import lombok.RequiredArgsConstructor;

/**
 * Aggregates one staged order's line items into an Order + Invoice. Pure computation, no
 * database access: StagedOrderItemReader has already loaded the lines for the whole page
 * and dropped orders that were invoiced in a prior run, which is what keeps the job safe
 * to re-trigger without needing skip/exception machinery.
 *
 * @StepScope (not a plain singleton) so businessDate can be late-bound from the run's
 * JobParameters - stamped onto each Invoice as issuedDate so it lines up with
 * dailySalesReportJob's issuedDate filter regardless of when the job physically runs.
 * The generated constructor picks up @Value here because lombok.config's
 * copyableAnnotations copies it from the field onto the parameter - without that,
 * @RequiredArgsConstructor would silently drop it and Spring would try (and fail) to
 * inject a LocalDate bean by type instead.
 */
@Component
@StepScope
@RequiredArgsConstructor
public class InvoiceAggregationProcessor implements ItemProcessor<StagedOrder, OrderInvoiceResult> {

    private final InvoiceCalculator invoiceCalculator;
    @Value("#{jobParameters['businessDate']}")
    private final LocalDate businessDate;

    @Override
    public OrderInvoiceResult process(StagedOrder stagedOrder) {
        List<OrderLineItemStaging> stagedLines = stagedOrder.lines();
        Order order = buildOrder(stagedOrder.orderId(), stagedLines);
        order.setInvoice(invoiceCalculator.calculate(order, businessDate));
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
