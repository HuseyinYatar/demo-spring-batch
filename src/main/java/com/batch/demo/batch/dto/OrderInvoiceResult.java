package com.batch.demo.batch.dto;

import java.util.List;

import com.batch.demo.domain.Order;
import com.batch.demo.domain.OrderLineItemStaging;

public record OrderInvoiceResult(Order order, List<OrderLineItemStaging> sourceStagingLines) {
}
