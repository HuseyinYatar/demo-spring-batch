package com.batch.demo.repository;

import java.math.BigDecimal;

/**
 * Interface projection for {@link InvoiceRepository#aggregateByIssuedDate}. Getter
 * names must match the JPQL query's {@code as} aliases exactly.
 */
public interface DailySalesAggregate {

    long getInvoiceCount();

    BigDecimal getTotalSubtotal();

    BigDecimal getTotalTax();

    BigDecimal getTotalAmount();
}
