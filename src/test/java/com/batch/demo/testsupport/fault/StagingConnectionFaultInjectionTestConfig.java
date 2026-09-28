package com.batch.demo.testsupport.fault;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * Same as StagingFaultInjectionTestConfig, but the trigger throws
 * SQLTransientConnectionException instead of a generic SQLException - simulating a
 * genuine, correctly-classified DB connection failure rather than an arbitrary
 * unclassified one, so it exercises ingestLineItemsWorkerStep's new
 * TransientDataAccessException/DataAccessResourceFailureException retry policy
 * (see IngestLineItemsStepConfig) instead of falling straight through as fatal.
 */
@TestConfiguration
public class StagingConnectionFaultInjectionTestConfig {

    @Bean
    public SqlInsertFaultTrigger stagingInsertFaultTrigger() {
        return new SqlInsertFaultTrigger("insert into order_line_item_staging", true);
    }

    @Bean
    public static BeanPostProcessor stagingConnectionFaultInjectingDataSourcePostProcessor(SqlInsertFaultTrigger trigger) {
        return FaultInjectingDataSourceSupport.wrapping(trigger);
    }
}
