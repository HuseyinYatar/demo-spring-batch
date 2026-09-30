package com.batch.demo;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.context.ActiveProfiles;

import io.micrometer.core.instrument.MeterRegistry;

import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ThreadPoolTaskExecutor.initialize() unconditionally builds a new ThreadPoolExecutor and
 * overwrites the previous one, and Spring calls it again itself through afterPropertiesSet().
 * Calling initialize() by hand inside the @Bean method therefore orphans the first pool
 * (never shut down, and anything bound to it - such as executor metrics - reads NaN).
 */
@SpringBootTest
@ActiveProfiles("test")
class BatchTaskExecutorLifecycleTest extends AbstractPostgresIntegrationTest {

    static final AtomicBoolean initializedBeforeSpringInit = new AtomicBoolean(false);

    @TestConfiguration
    static class InitializationProbeConfig {
        /**
         * Runs after the @Bean method returns but before afterPropertiesSet(), so an
         * executor that already has a pool at this point was initialized by hand.
         */
        @Bean
        static BeanPostProcessor batchTaskExecutorInitializationProbe() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessBeforeInitialization(Object bean, String beanName) {
                    if ("batchTaskExecutor".equals(beanName) && bean instanceof ThreadPoolTaskExecutor executor) {
                        try {
                            executor.getThreadPoolExecutor();
                            initializedBeforeSpringInit.set(true);
                        } catch (IllegalStateException notYetInitialized) {
                            // expected: Spring's own afterPropertiesSet() hasn't run yet
                        }
                    }
                    return bean;
                }
            };
        }
    }

    @Autowired
    private ThreadPoolTaskExecutor batchTaskExecutor;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void executorIsInitializedOnlyBySpring() {
        assertThat(initializedBeforeSpringInit)
                .as("batchTaskExecutor must not be initialize()d inside its @Bean method")
                .isFalse();
    }

    /**
     * The executor metrics come from Boot's TaskExecutorMetricsAutoConfiguration (tagged
     * with the bean name), not from anything in this project. Micrometer's gauges hold
     * their target through a WeakReference, so they read NaN as soon as the monitored
     * ThreadPoolExecutor is garbage collected. Forcing GC before reading proves they are
     * bound to the pool batchTaskExecutor really uses, under the tag the Grafana
     * dashboards query.
     */
    @Test
    void executorGaugesStayBoundToTheLivePoolAcrossGc() {
        for (int i = 0; i < 3; i++) {
            System.gc();
        }

        double maxPoolSize = meterRegistry.get("executor.pool.max")
                .tag("name", "batchTaskExecutor")
                .gauge()
                .value();

        assertThat(maxPoolSize).isEqualTo(batchTaskExecutor.getMaxPoolSize());
    }
}
