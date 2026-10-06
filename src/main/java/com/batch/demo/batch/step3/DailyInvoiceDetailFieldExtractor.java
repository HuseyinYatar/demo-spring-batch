package com.batch.demo.batch.step3;

import org.springframework.batch.infrastructure.item.file.transform.FieldExtractor;
import org.springframework.stereotype.Component;

/**
 * Mirrors InvoiceSummaryFieldExtractor's shape, but reads a {@link DailyInvoiceDetail} row -
 * dailySalesReportJob runs after the fact, over already-invoiced orders, so there's no
 * in-flight OrderInvoiceResult DTO to extract from here.
 */
@Component
public class DailyInvoiceDetailFieldExtractor implements FieldExtractor<DailyInvoiceDetail> {

    @Override
    public Object[] extract(DailyInvoiceDetail detail) {
        return new Object[] {
                detail.orderNumber(),
                detail.customerName(),
                detail.invoiceNumber(),
                detail.subtotal(),
                detail.taxAmount(),
                detail.totalAmount(),
                detail.issuedDate()
        };
    }
}
