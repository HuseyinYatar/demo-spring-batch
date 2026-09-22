package com.batch.demo.web;

import java.util.UUID;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobRestartException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.batch.demo.web.dto.JobExecutionStatusResponse;
import com.batch.demo.web.dto.JobLaunchResponse;

import lombok.RequiredArgsConstructor;

/**
 * Deliberately thin: only launches the already-assembled {@link Job} bean and reports
 * status. No job-building logic lives here (single responsibility).
 */
@RestController
@RequestMapping("/api/batch/jobs")
@RequiredArgsConstructor
public class BatchJobController {

    private final JobLauncher jobLauncher;
    private final Job orderProcessingJob;
    private final JobExplorer jobExplorer;

    @PostMapping("/order-processing")
    public ResponseEntity<JobLaunchResponse> launch()
            throws JobExecutionAlreadyRunningException, JobRestartException,
            JobInstanceAlreadyCompleteException, InvalidJobParametersException {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("runId", UUID.randomUUID().toString())
                .addLong("startedAtEpochMs", System.currentTimeMillis())
                .toJobParameters();

        JobExecution execution = jobLauncher.run(orderProcessingJob, jobParameters);

        return ResponseEntity.ok(new JobLaunchResponse(
                execution.getId(),
                execution.getStatus().name(),
                execution.getStartTime()));
    }

    @GetMapping("/executions/{id}")
    public ResponseEntity<JobExecutionStatusResponse> getExecution(@PathVariable Long id) {
        JobExecution execution = jobExplorer.getJobExecution(id);
        if (execution == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(JobExecutionStatusResponse.from(execution));
    }
}
