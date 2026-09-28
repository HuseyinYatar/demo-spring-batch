package com.batch.demo.testsupport;

import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.test.JobLauncherTestUtils;

import com.batch.demo.batch.control.JobControlService;

/**
 * A JobExecution-returning method declared directly on a @SpringBatchTest test class
 * gets misdetected by JobScopeTestExecutionListener (which scans the test class for a
 * job-execution-provider method by return type, not by name - the same issue
 * StepScopeTestExecutionListener causes for StepExecution-returning methods, see
 * StepExecutions) - kept as a separate utility class specifically to avoid that.
 */
public final class RunningJobs {

    private RunningJobs() {
    }

    /**
     * Launches orderProcessingJob on a background thread (this app's JobLauncher is
     * synchronous, so a running job can only be observed/stopped from another thread),
     * waits for it to reach a running state, requests a stop, and returns the final
     * JobExecution once the launch call returns.
     */
    public static JobExecution launchAndStop(JobLauncherTestUtils jobLauncherTestUtils, JobExplorer jobExplorer,
            JobControlService jobControlService, ExecutorService executor, JobParameters jobParameters)
            throws Exception {
        Future<JobExecution> future = executor.submit(() -> jobLauncherTestUtils.launchJob(jobParameters));

        JobExecution running = awaitRunning(jobExplorer, jobParameters, Instant.now().plusSeconds(5));
        jobControlService.stop(running.getId());

        return future.get(15, TimeUnit.SECONDS);
    }

    private static JobExecution awaitRunning(JobExplorer jobExplorer, JobParameters jobParameters, Instant deadline)
            throws InterruptedException {
        while (Instant.now().isBefore(deadline)) {
            JobExecution candidate = jobExplorer.getLastJobExecution("orderProcessingJob", jobParameters);
            if (candidate != null && candidate.getStatus().isRunning()) {
                return candidate;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("Job never reached a running state within the deadline");
    }
}
