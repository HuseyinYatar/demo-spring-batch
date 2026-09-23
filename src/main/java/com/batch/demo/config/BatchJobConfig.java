package com.batch.demo.config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.batch.demo.batch.reject.RejectedRecordSink;
import com.batch.demo.batch.step2.FlakyOrderPersistenceSimulator;
import com.batch.demo.batch.step2.InvoiceSummaryPartitionPaths;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;

/**
 * {@code @EnableBatchProcessing} + {@link EnableJdbcJobRepository} together back the
 * JobRepository with the PostgreSQL BATCH_* tables (see spring.sql.init.* in
 * application.properties for schema creation) instead of Boot's default in-memory
 * repository, so job/step execution history survives past the request that launched
 * it. Declaring {@code @EnableBatchProcessing} here (rather than relying on Boot's
 * own, which backs off once any {@code @EnableBatchProcessing} is found elsewhere)
 * is what makes {@link EnableJdbcJobRepository} take effect.
 */
@Configuration
@EnableBatchProcessing
@EnableJdbcJobRepository
public class BatchJobConfig {

    @Bean
    public ThreadPoolTaskExecutor batchTaskExecutor(BatchProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(properties.getPartitionGridSize());
        executor.setMaxPoolSize(properties.getPartitionGridSize());
        executor.setThreadNamePrefix("batch-partition-");
        executor.initialize();
        return executor;
    }

    /**
     * Boot doesn't auto-instrument ThreadPoolTaskExecutor beans (no
     * TaskExecutorMetricsAutoConfiguration in this version, confirmed via jar
     * inspection), so batchTaskExecutor's underlying ThreadPoolExecutor is bound to
     * Micrometer manually here - exposes executor_active_threads/
     * executor_pool_size_threads/executor_queued_tasks etc. tagged
     * name="batchTaskExecutor" on /actuator/prometheus. Returning the raw
     * ThreadPoolExecutor as its own bean (rather than just calling
     * ExecutorServiceMetrics.monitor(...) inline inside batchTaskExecutor) matters:
     * Micrometer's Gauge holds its target via a WeakReference, and confirmed live
     * that without an independent strong reference in Spring's own singleton
     * registry, the gauges intermittently report NaN once the executor becomes only
     * weakly reachable - this bean's return value is that strong reference.
     */
    @Bean
    public ThreadPoolExecutor batchThreadPoolExecutorMetrics(ThreadPoolTaskExecutor batchTaskExecutor,
                                                               MeterRegistry meterRegistry) {
        ThreadPoolExecutor threadPoolExecutor = batchTaskExecutor.getThreadPoolExecutor();
        ExecutorServiceMetrics.monitor(meterRegistry, threadPoolExecutor, "batchTaskExecutor");
        return threadPoolExecutor;
    }

    @Bean
    public JobExecutionListener perRunStateResetListener(RejectedRecordSink rejectedRecordSink,
                                                           FlakyOrderPersistenceSimulator flakySimulator,
                                                           BatchProperties properties) {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                rejectedRecordSink.reset();
                flakySimulator.reset();
                // Stale partition files from a previous run (especially one with a
                // different batch.partition-grid-size) must not be picked up by this
                // run's mergeInvoiceSummaryStep alongside the fresh ones.
                for (Path partitionFile : InvoiceSummaryPartitionPaths.listPartitionFiles(properties.getInvoiceSummaryOutputPath())) {
                    try {
                        Files.deleteIfExists(partitionFile);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Unable to delete stale partition file " + partitionFile, e);
                    }
                }
            }
        };
    }

    @Bean
    public Job orderProcessingJob(JobRepository jobRepository,
                                   Step ingestLineItemsStep,
                                   Step buildInvoicesStep,
                                   Step mergeInvoiceSummaryStep,
                                   JobExecutionListener perRunStateResetListener) {
        return new JobBuilder("orderProcessingJob", jobRepository)
                .listener(perRunStateResetListener)
                .start(ingestLineItemsStep)
                .next(buildInvoicesStep)
                .next(mergeInvoiceSummaryStep)
                .build();
    }
}
