package com.batch.demo.testsupport.fault;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class OrderFaultInjectionTestConfig {

    @Bean
    public SqlInsertFaultTrigger orderInsertFaultTrigger() {
        return new SqlInsertFaultTrigger("insert into orders");
    }

    @Bean
    public static BeanPostProcessor orderFaultInjectingDataSourcePostProcessor(SqlInsertFaultTrigger trigger) {
        return FaultInjectingDataSourceSupport.wrapping(trigger);
    }
}
