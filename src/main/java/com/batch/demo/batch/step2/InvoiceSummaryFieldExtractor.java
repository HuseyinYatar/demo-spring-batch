package com.batch.demo.batch.step2;

import org.springframework.batch.infrastructure.item.file.transform.FieldExtractor;
import org.springframework.stereotype.Component;

import com.batch.demo.batch.dto.OrderInvoiceResult;
import com.batch.demo.domain.Invoice;
import com.batch.demo.domain.Order;

@Component
public class InvoiceSummaryFieldExtractor implements FieldExtractor<OrderInvoiceResult> {

    @Override
    public Object[] extract(OrderInvoiceResult result) {
        Order order = result.order();
        Invoice invoice = order.getInvoice();
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
