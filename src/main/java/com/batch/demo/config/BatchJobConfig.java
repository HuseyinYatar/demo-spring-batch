package com.batch.demo.config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.batch.demo.batch.reject.RejectedRecordSink;
import com.batch.demo.batch.step2.FlakyOrderPersistenceSimulator;
import com.batch.demo.batch.step2.InvoiceSummaryPartitionPaths;

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
    public TaskExecutor batchTaskExecutor(BatchProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(properties.getPartitionGridSize());
        executor.setMaxPoolSize(properties.getPartitionGridSize());
        executor.setThreadNamePrefix("batch-partition-");
        executor.initialize();
        return executor;
    }

    @Bean
    public JobExecutionListener perRunStateResetListener(RejectedRecordSink rejectedRecordSink,
                                                           FlakyOrderPersistenceSimulator flakySimulator,
                                                           BatchProperties properties,
                                                           JobExplorer jobExplorer) {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                rejectedRecordSink.reset();
                flakySimulator.reset();

                // A restart of a previously-attempted execution must NOT have its
                // partition files deleted here: buildInvoicesWorkerStep's
                // invoiceSummaryCsvItemWriter resumes appending to the same file
                // (restored from its own saved ExecutionContext) and expects it to
                // still exist. Only a genuinely fresh JobInstance - which can't have
                // touched these files itself - gets this stale-file cleanup, guarding
                // against leftovers from an unrelated prior run (e.g. a different
                // batch.partition-grid-size).
                boolean isRestart = jobExplorer.getJobExecutions(jobExecution.getJobInstance()).size() > 1;
                if (isRestart) {
                    return;
                }
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
