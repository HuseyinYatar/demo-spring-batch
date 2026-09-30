package com.batch.demo.config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.TaskExecutor;
import org.springframework.core.task.support.ContextPropagatingTaskDecorator;
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
        // Without this, the tracing span/observation active on the manager
        // thread (the one running ingestLineItemsStep/buildInvoicesStep and
        // submitting partitions) never reaches the worker threads this
        // executor runs partitions on - each partition's step/chunk spans
        // would show up as disconnected, parentless traces instead of
        // nesting under the job/step span. Boot doesn't apply this
        // decorator automatically here because batchTaskExecutor is a
        // manually-declared bean, not the auto-configured TaskExecutor.
        executor.setTaskDecorator(new ContextPropagatingTaskDecorator());
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

    /**
     * DefaultBatchConfiguration only auto-populates a JobRegistry the first time
     * something (here, its own JobOperator bean) asks for one - via
     * {@code applicationContext.getBeansOfType(Job.class)} taken as a ONE-SHOT
     * snapshot at that exact moment (confirmed by decompiling
     * DefaultBatchConfiguration.getJobRegistry()), not a live view. That snapshot
     * structurally can never include dailyPipelineJob or dailySalesReportJob (see
     * DailyPipelineJobConfig): both depend on a JobStep, which depends on JobOperator
     * itself, so by definition neither Job bean can exist yet at the moment
     * JobOperator's own creation takes that snapshot. Confirmed the hard way:
     * JobControlService.restart(...) against a dailyPipelineJob execution threw
     * "IllegalArgumentException: The Job must not be null" from deep inside
     * SimpleJobOperator.restart(), because jobRegistry.getJob("dailyPipelineJob")
     * silently found nothing.
     *
     * The fix is only this one bean: once a JobRegistry bean exists,
     * DefaultBatchConfiguration's {@code getIfAvailable(...)} call uses it directly
     * instead of auto-populating its own, AND Spring Boot's own batch autoconfiguration
     * separately supplies a JobRegistrySmartInitializingSingleton that discovers this
     * bean by type and populates it once every singleton in the context - including
     * dailyPipelineJob - has finished being created, sidestepping the ordering problem
     * entirely. Do NOT also declare a JobRegistrySmartInitializingSingleton bean here:
     * confirmed the hard way that doing so registers every Job bean a second time,
     * against the same registry, throwing DuplicateJobException at startup - Boot
     * already provides one.
     */
    @Bean
    public JobRegistry jobRegistry() {
        return new MapJobRegistry();
    }

    @Bean
    public JobExecutionListener perRunStateResetListener(RejectedRecordSink rejectedRecordSink,
                                                           FlakyOrderPersistenceSimulator flakySimulator,
                                                           BatchProperties properties,
                                                           JobExplorer jobExplorer) {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                flakySimulator.reset();

                // A restart of a previously-attempted execution must NOT have its
                // rejects file truncated or its partition files deleted here:
                // rejected-rows.csv already holds rows skipped by the failed attempt,
                // and buildInvoicesWorkerStep's invoiceSummaryCsvItemWriter resumes
                // appending to the same partition file (restored from its own saved
                // ExecutionContext), expecting it to still exist. Only a genuinely
                // fresh JobInstance - which can't have touched these files itself -
                // gets the reset/cleanup, guarding against leftovers from an
                // unrelated prior run (e.g. a different batch.partition-grid-size).
                boolean isRestart = jobExplorer.getJobExecutions(jobExecution.getJobInstance()).size() > 1;
                if (isRestart) {
                    return;
                }
                rejectedRecordSink.reset();
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

    /**
     * {@code @Primary} matters once {@code dailyPipelineJob}/{@code dailySalesReportJob}
     * (see DailyPipelineJobConfig) add more {@link Job} beans to the context: every
     * integration test's {@code @SpringBatchTest}-provided {@code JobLauncherTestUtils}
     * gets its job field populated via {@code ObjectProvider.ifUnique(...)} (confirmed
     * via javap on BatchTestContextBeanPostProcessor), which silently leaves the field
     * null - no exception - the moment there's more than one non-primary {@link Job}
     * candidate. Without this annotation, every existing test calling
     * {@code jobLauncherTestUtils.launchJob(...)} would NPE.
     */
    @Bean
    @Primary
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
