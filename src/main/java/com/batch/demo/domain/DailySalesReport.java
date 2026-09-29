package com.batch.demo.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * One row per business date, produced by {@code dailySalesReportJob}'s summary step.
 * Upserted rather than blindly inserted (see DailySalesSummaryTasklet) so a re-run for
 * an already-reported date is idempotent instead of tripping the unique constraint.
 */
@Entity
@Table(name = "daily_sales_report", uniqueConstraints = @UniqueConstraint(columnNames = "business_date"))
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DailySalesReport {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private LocalDate businessDate;

    private long invoiceCount;

    private BigDecimal totalSubtotal;

    private BigDecimal totalTax;

    private BigDecimal totalAmount;

    private String topCustomerName;

    private BigDecimal topCustomerTotal;

    private Instant generatedAt;
}
