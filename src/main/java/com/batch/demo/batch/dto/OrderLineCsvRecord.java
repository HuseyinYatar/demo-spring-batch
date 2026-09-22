package com.batch.demo.batch.dto;

import java.math.BigDecimal;
import java.time.LocalDate;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OrderLineCsvRecord {

    private String orderId;
    private String customerId;
    private String customerName;
    private String productId;
    private String productName;
    private Integer quantity;
    private BigDecimal unitPrice;
    private LocalDate orderDate;
}
