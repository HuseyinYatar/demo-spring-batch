package com.batch.demo.batch.dto;

import java.util.List;

import com.batch.demo.domain.OrderLineItemStaging;

/**
 * One order's unprocessed staging lines, loaded together with the rest of its page by
 * StagedOrderItemReader so the processor needs no database access of its own.
 */
public record StagedOrder(String orderId, List<OrderLineItemStaging> lines) {
}
