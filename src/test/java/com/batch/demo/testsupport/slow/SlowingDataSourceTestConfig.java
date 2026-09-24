package com.batch.demo.testsupport.slow;

import javax.sql.DataSource;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * A BeanPostProcessor, not a competing @Bean DataSource - see
 * FaultInjectingDataSourceTestConfig for why a same-type @Bean here would make Boot's
 * own DataSourceConfiguration back off instead of ever creating the real DataSource.
 */
@TestConfiguration
public class SlowingDataSourceTestConfig {

    @Bean
    public SlowStagingInsertTrigger slowStagingInsertTrigger() {
        return new SlowStagingInsertTrigger();
    }

    @Bean
    public static BeanPostProcessor slowingDataSourcePostProcessor(SlowStagingInsertTrigger trigger) {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
                if (bean instanceof DataSource dataSource && !(bean instanceof SlowingDataSource)) {
                    return new SlowingDataSource(dataSource, trigger);
                }
                return bean;
            }
        };
    }
}
