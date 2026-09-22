package com.batch.demo.batch.step2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;

import org.springframework.stereotype.Component;

import com.batch.demo.config.BatchProperties;
import com.batch.demo.domain.Invoice;
import com.batch.demo.domain.Order;
import com.batch.demo.domain.OrderLineItem;

/**
 * Pure tax/total computation, kept out of {@link InvoiceAggregationProcessor} so that
 * class stays a thin orchestrator (single responsibility).
 */
@Component
public class InvoiceCalculator {

    private final BigDecimal taxRate;

    public InvoiceCalculator(BatchProperties properties) {
        this.taxRate = properties.getTaxRate();
    }

    public Invoice calculate(Order order) {
        BigDecimal subtotal = order.getLineItems().stream()
                .map(OrderLineItem::getLineTotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .setScale(2, RoundingMode.HALF_UP);

        BigDecimal tax = subtotal.multiply(taxRate).setScale(2, RoundingMode.HALF_UP);
        BigDecimal total = subtotal.add(tax);

        return Invoice.builder()
                .invoiceNumber("INV-" + order.getOrderNumber())
                .subtotal(subtotal)
                .taxAmount(tax)
                .totalAmount(total)
                .issuedDate(LocalDate.now())
                .build();
    }
}
