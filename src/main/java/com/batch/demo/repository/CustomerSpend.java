package com.batch.demo.repository;

import java.math.BigDecimal;

/**
 * Interface projection for {@link InvoiceRepository#findTopCustomersByIssuedDate}.
 * Getter names must match the JPQL query's {@code as} aliases exactly.
 */
public interface CustomerSpend {

    String getCustomerName();

    BigDecimal getTotalSpend();
}
