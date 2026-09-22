package com.batch.demo.config;

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

import com.batch.demo.batch.reject.RejectedRecordSink;
import com.batch.demo.batch.step2.FlakyOrderPersistenceSimulator;

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
    public JobExecutionListener perRunStateResetListener(RejectedRecordSink rejectedRecordSink,
                                                           FlakyOrderPersistenceSimulator flakySimulator) {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                rejectedRecordSink.reset();
                flakySimulator.reset();
            }
        };
    }

    @Bean
    public Job orderProcessingJob(JobRepository jobRepository,
                                   Step ingestLineItemsStep,
                                   Step buildInvoicesStep,
                                   JobExecutionListener perRunStateResetListener) {
        return new JobBuilder("orderProcessingJob", jobRepository)
                .listener(perRunStateResetListener)
                .start(ingestLineItemsStep)
                .next(buildInvoicesStep)
                .build();
    }
}
