package com.batch.demo.testsupport.fault;

import javax.sql.DataSource;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * Wraps the real DataSource via a BeanPostProcessor rather than declaring a
 * competing @Bean DataSource - a same-type @Bean method here would make Boot's own
 * DataSourceConfiguration back off (@ConditionalOnMissingBean(DataSource.class)),
 * so the real DataSource would never get created at all. Post-processing the
 * already-created bean sidesteps that entirely.
 */
@TestConfiguration
public class FaultInjectingDataSourceTestConfig {

    @Bean
    public OrderInsertFaultTrigger orderInsertFaultTrigger() {
        return new OrderInsertFaultTrigger();
    }

    @Bean
    public static BeanPostProcessor faultInjectingDataSourcePostProcessor(OrderInsertFaultTrigger trigger) {
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
