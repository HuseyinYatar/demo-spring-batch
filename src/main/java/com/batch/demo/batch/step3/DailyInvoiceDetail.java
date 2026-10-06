package com.batch.demo.batch.step3;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * One row of dailySalesReportJob's detail CSV: an invoice joined with its order's number and
 * customer, read as plain values rather than as an Invoice entity (see
 * DailySalesReportStepConfig.dailyInvoiceItemReader for why).
 */
public record DailyInvoiceDetail(String orderNumber,
                                 String customerName,
                                 String invoiceNumber,
                                 BigDecimal subtotal,
                                 BigDecimal taxAmount,
                                 BigDecimal totalAmount,
                                 LocalDate issuedDate) {
}
