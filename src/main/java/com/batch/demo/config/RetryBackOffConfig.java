package com.batch.demo.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialRandomBackOffPolicy;
import org.springframework.retry.backoff.Sleeper;

@Configuration
public class RetryBackOffConfig {

    /**
     * Exponential backoff with jitter, shared by both worker steps. Jitter
     * (ExponentialRandomBackOffPolicy, not plain ExponentialBackOffPolicy) matters
     * here specifically: every partition runs concurrently, so a DB blip fails
     * several of them at once, and without randomization they would all retry in
     * lockstep against a database that is still recovering.
     */
    @Bean
    public BackOffPolicy retryBackOffPolicy(BatchProperties properties) {
        ExponentialRandomBackOffPolicy policy = new ExponentialRandomBackOffPolicy();
        policy.setInitialInterval(properties.getRetryBackoffInitialIntervalMs());
        policy.setMultiplier(properties.getRetryBackoffMultiplier());
        policy.setMaxInterval(properties.getRetryBackoffMaxIntervalMs());
        policy.setSleeper(new LoggingSleeper());
        return policy;
    }

    /**
     * Logs the actual (jittered) wait. A RetryListener can't report it: onError runs
     * before the policy picks the interval, and the sleeper is the only place the
     * chosen value is visible. It sits right after the retry listeners' WARN line
     * in the same partition thread.
     */
    private static class LoggingSleeper implements Sleeper {

        private static final Logger log = LoggerFactory.getLogger(LoggingSleeper.class);

        @Override
        public void sleep(long backOffPeriod) throws InterruptedException {
            log.warn("Backing off {} ms before retrying", backOffPeriod);
            Thread.sleep(backOffPeriod);
        }
    }
}
