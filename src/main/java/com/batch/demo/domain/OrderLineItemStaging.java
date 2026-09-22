package com.batch.demo.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "order_line_item_staging")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderLineItemStaging {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String orderId;

    private String customerId;

    private String customerName;

    private String productId;

    private String productName;

    private Integer quantity;

    private BigDecimal unitPrice;

    private LocalDate orderDate;

    @Builder.Default
    private boolean processed = false;
}
