package com.batch.demo.testsupport.fault;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * Same as OrderFaultInjectionTestConfig, but the trigger throws
 * SQLTransientConnectionException instead of a generic SQLException - simulating a
 * genuine, correctly-classified DB connection failure so it exercises
 * buildInvoicesWorkerStep's TransientDataAccessException/DataAccessResourceFailureException
 * retry policy (see BuildInvoicesStepConfig) rather than falling straight through as
 * fatal.
 */
@TestConfiguration
public class OrderConnectionFaultInjectionTestConfig {

    @Bean
    public SqlInsertFaultTrigger orderInsertFaultTrigger() {
        return new SqlInsertFaultTrigger("insert into orders", true);
    }

    @Bean
    public static BeanPostProcessor orderConnectionFaultInjectingDataSourcePostProcessor(SqlInsertFaultTrigger trigger) {
        return FaultInjectingDataSourceSupport.wrapping(trigger);
    }
}
