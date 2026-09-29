package com.batch.demo.batch.step3;

import org.springframework.batch.infrastructure.item.file.transform.FieldExtractor;
import org.springframework.stereotype.Component;

import com.batch.demo.domain.Invoice;
import com.batch.demo.domain.Order;

/**
 * Mirrors InvoiceSummaryFieldExtractor's shape, but reads straight off a persisted
 * {@link Invoice} - dailySalesReportJob runs after the fact, over already-invoiced
 * orders, so there's no in-flight OrderInvoiceResult DTO to extract from here.
 */
@Component
public class DailyInvoiceDetailFieldExtractor implements FieldExtractor<Invoice> {

    @Override
    public Object[] extract(Invoice invoice) {
        Order order = invoice.getOrder();
        return new Object[] {
                order.getOrderNumber(),
                order.getCustomerName(),
                invoice.getInvoiceNumber(),
                invoice.getSubtotal(),
                invoice.getTaxAmount(),
                invoice.getTotalAmount(),
                invoice.getIssuedDate()
        };
    }
}
