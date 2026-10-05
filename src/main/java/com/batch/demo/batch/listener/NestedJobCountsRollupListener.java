package com.batch.demo.batch.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.job.JobParametersExtractor;

import com.batch.demo.batch.support.JobCounts;

import lombok.RequiredArgsConstructor;

/**
 * Copies a nested job's item counts onto the JobStep that ran it, the same way a
 * partitioned manager step carries the sum of its workers' counts.
 *
 * A JobStep does no item processing of its own, and the nested job's step executions
 * belong to the nested job's own JobExecution - so without this, a JobExecution made of
 * JobSteps (dailyPipelineJob) reports 0 reads/writes/skips no matter how much work its
 * nested jobs did. Runs in afterStep, so it also covers a nested job that failed partway
 * (the work it did is real), and happens before AbstractStep persists the StepExecution.
 *
 * JobStep exposes neither the nested JobExecution nor its id, so it is looked up through
 * the same extractor the step used to build the nested job's parameters - the identifying
 * ones are all it takes to find the nested JobInstance.
 */
@RequiredArgsConstructor
public class NestedJobCountsRollupListener implements StepExecutionListener {

    private final JobExplorer jobExplorer;
    private final Job nestedJob;
    private final JobParametersExtractor parametersExtractor;

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        JobParameters nestedParameters = parametersExtractor.getJobParameters(nestedJob, stepExecution);
        JobExecution nested = jobExplorer.getLastJobExecution(nestedJob.getName(), nestedParameters);

        // The nested JobExecution is created after this step's own JobExecution, so a
        // lower id means jobOperator.start(...) never created one - it was rejected (a
        // standalone run of the same instance already COMPLETED, say) - and the
        // execution found is some earlier run's, whose counts are not this step's work.
        if (nested != null && nested.getId() > stepExecution.getJobExecutionId()) {
            JobCounts.of(nested).applyTo(stepExecution);
        }
        return null;
    }
}
