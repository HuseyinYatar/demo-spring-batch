package com.batch.demo.testsupport.fault;

import javax.sql.DataSource;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

/**
 * Shared by every per-table fault-injection @TestConfiguration (OrderFaultInjectionTestConfig,
 * StagingFaultInjectionTestConfig, ...): a BeanPostProcessor, not a competing @Bean
 * DataSource - a same-type @Bean would make Boot's own DataSourceConfiguration back
 * off (@ConditionalOnMissingBean(DataSource.class)), so the real DataSource would
 * never get created at all. Post-processing the already-created bean sidesteps that.
 */
public final class FaultInjectingDataSourceSupport {

    private FaultInjectingDataSourceSupport() {
    }

    public static BeanPostProcessor wrapping(SqlInsertFaultTrigger trigger) {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
                if (bean instanceof DataSource dataSource && !(bean instanceof FaultInjectingDataSource)) {
                    return new FaultInjectingDataSource(dataSource, trigger);
                }
                return bean;
            }
        };
    }
}
