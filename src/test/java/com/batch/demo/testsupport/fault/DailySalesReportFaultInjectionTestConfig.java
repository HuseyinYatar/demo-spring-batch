package com.batch.demo.testsupport.fault;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * Mirrors OrderFaultInjectionTestConfig, targeting daily_sales_report's insert instead
 * of orders' - reuses the same generic SqlInsertFaultTrigger/FaultInjectingDataSource
 * machinery rather than a bespoke trigger class.
 */
@TestConfiguration
public class DailySalesReportFaultInjectionTestConfig {

    @Bean
    public SqlInsertFaultTrigger dailySalesReportInsertFaultTrigger() {
        return new SqlInsertFaultTrigger("insert into daily_sales_report");
    }

    @Bean
    public static BeanPostProcessor dailySalesReportFaultInjectingDataSourcePostProcessor(
            SqlInsertFaultTrigger dailySalesReportInsertFaultTrigger) {
        return FaultInjectingDataSourceSupport.wrapping(dailySalesReportInsertFaultTrigger);
    }
}
