package com.batch.demo.testsupport.fault;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class StagingFaultInjectionTestConfig {

    @Bean
    public SqlInsertFaultTrigger stagingInsertFaultTrigger() {
        return new SqlInsertFaultTrigger("insert into order_line_item_staging");
    }

    @Bean
    public static BeanPostProcessor stagingFaultInjectingDataSourcePostProcessor(SqlInsertFaultTrigger trigger) {
        return FaultInjectingDataSourceSupport.wrapping(trigger);
    }
}
