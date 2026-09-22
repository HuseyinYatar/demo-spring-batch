package com.batch.demo.batch.step2;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

import com.batch.demo.config.BatchProperties;
import com.batch.demo.domain.Invoice;
import com.batch.demo.domain.Order;
import com.batch.demo.domain.OrderLineItem;

class InvoiceCalculatorTest {

    private final BatchProperties properties = new BatchProperties();
    private final InvoiceCalculator calculator = new InvoiceCalculator(properties);

    @Test
    void computesSubtotalTaxAndTotalFromLineItems() {
        properties.setTaxRate(new BigDecimal("0.18"));

        Order order = Order.builder().orderNumber("ORD-1001").build();
        order.addLineItem(OrderLineItem.builder().lineTotal(new BigDecimal("1598.00")).build());
        order.addLineItem(OrderLineItem.builder().lineTotal(new BigDecimal("597.00")).build());

        Invoice invoice = calculator.calculate(order);

        assertThat(invoice.getSubtotal()).isEqualByComparingTo("2195.00");
        assertThat(invoice.getTaxAmount()).isEqualByComparingTo("395.10");
        assertThat(invoice.getTotalAmount()).isEqualByComparingTo("2590.10");
        assertThat(invoice.getInvoiceNumber()).isEqualTo("INV-ORD-1001");
    }
}
