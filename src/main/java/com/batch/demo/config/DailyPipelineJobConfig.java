package com.batch.demo.config;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.job.DefaultJobParametersExtractor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * dailyPipelineJob orchestrates the existing orderProcessingJob and the new
 * dailySalesReportJob (see DailySalesReportStepConfig) by nesting each as a JobStep -
 * Spring Batch's job-of-jobs composition pattern, unused elsewhere in this codebase.
 *
 * JobStep requires a JobOperator (not a JobLauncher, unlike pre-6.0 Spring Batch),
 * confirmed via javap on org.springframework.batch.core.step.job.JobStep
 * (afterPropertiesSet asserts "A JobOperator must be provided"). The JobOperator bean
 * used here is the exact same one JobControlService already depends on for
 * stop/restart/abandon - provided for free by DefaultBatchConfiguration.
 *
 * JobOperator's own default TaskExecutor is a SyncTaskExecutor (confirmed via javap on
 * DefaultBatchConfiguration.getTaskExecutor()), so jobOperator.start(childJob, params)
 * inside JobStep.doExecute() blocks the calling thread until the child job reaches a
 * terminal status - .start(...).next(...) below is genuinely sequential, not
 * fire-and-forget.
 *
 * Each extractor sets useAllParentParameters(false) before setKeys(...):
 * DefaultJobParametersExtractor defaults useAllParentParameters to true, which would
 * copy every parent JobParameter into the child job's parameters regardless of
 * setKeys(...) (confirmed by reading the decompiled getJobParameters(...) body) - a
 * future orchestrator-only parameter could then silently change a child job's identity.
 * With it false, orderProcessingJobStep's child only ever sees {businessDate,
 * inputFile} - the exact identifying parameters orderProcessingJob already uses for its
 * own idempotency (see BatchJobController) - and dailySalesReportJobStep's child only
 * ever sees {businessDate}.
 *
 * {@code @Qualifier} on every {@link Job}-typed parameter below is not decorative:
 * with orderProcessingJob marked {@code @Primary} (see BatchJobConfig, required so
 * JobLauncherTestUtils's ambiguous-type autowiring keeps working in every existing
 * test), Spring's autowire-candidate resolution consults {@code @Primary} BEFORE
 * falling back to matching the injection point's name against a bean name - so without
 * an explicit qualifier here, the {@code Job dailySalesReportJob} parameter would
 * silently receive the {@code @Primary} orderProcessingJob bean instead (confirmed the
 * hard way: dailySalesReportJobStep ran a second, duplicate orderProcessingJob execution
 * instead of dailySalesReportJob, with no startup error - Spring never treats this as a
 * misconfiguration since @Primary successfully resolves the ambiguity, just not to the
 * bean the parameter name implies).
 */
@Configuration
public class DailyPipelineJobConfig {

    @Bean
    public Step orderProcessingJobStep(JobRepository jobRepository, JobOperator jobOperator,
                                        @Qualifier("orderProcessingJob") Job orderProcessingJob) {
        DefaultJobParametersExtractor extractor = new DefaultJobParametersExtractor();
        extractor.setUseAllParentParameters(false);
        extractor.setKeys(new String[] {"businessDate", "inputFile"});
        return new StepBuilder("orderProcessingJobStep", jobRepository)
                .job(orderProcessingJob)
                .operator(jobOperator)
                .parametersExtractor(extractor)
                .build();
    }

    @Bean
    public Step dailySalesReportJobStep(JobRepository jobRepository, JobOperator jobOperator,
                                         @Qualifier("dailySalesReportJob") Job dailySalesReportJob) {
        DefaultJobParametersExtractor extractor = new DefaultJobParametersExtractor();
        extractor.setUseAllParentParameters(false);
        extractor.setKeys(new String[] {"businessDate"});
        return new StepBuilder("dailySalesReportJobStep", jobRepository)
                .job(dailySalesReportJob)
                .operator(jobOperator)
                .parametersExtractor(extractor)
                .build();
    }

    @Bean
    public Job dailyPipelineJob(JobRepository jobRepository,
                                 Step orderProcessingJobStep,
                                 Step dailySalesReportJobStep) {
        return new JobBuilder("dailyPipelineJob", jobRepository)
                .start(orderProcessingJobStep)
                .next(dailySalesReportJobStep)
                .build();
    }
}
