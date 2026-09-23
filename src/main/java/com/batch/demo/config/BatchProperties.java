package com.batch.demo.config;

import java.math.BigDecimal;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties(prefix = "batch")
public class BatchProperties {

    private String inputCsvPath = "classpath:data/order-line-items.csv";
    private String rejectsFilePath = "rejected-rows.csv";
    private String invoiceSummaryOutputPath = "invoice-summary.csv";
    private BigDecimal taxRate = new BigDecimal("0.18");
    private int chunkSize =100;
    private int skipLimit = 20;
    private int retryLimit = 3;
    private boolean simulateTransientWriteFailures = true;
    private int partitionGridSize = 6;
    private int orderIdPageSize = 500;
}
