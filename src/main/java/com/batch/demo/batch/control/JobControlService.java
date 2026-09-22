package com.batch.demo.batch.control;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobRestartException;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.stereotype.Service;

import com.batch.demo.web.dto.JobExecutionStatusResponse;

import lombok.RequiredArgsConstructor;

/**
 * Wraps {@link JobOperator} for stopping, restarting and abandoning executions.
 * Checked exceptions are left to propagate - translation to HTTP status codes is
 * BatchOperationExceptionHandler's job, not this service's.
 */
@Service
@RequiredArgsConstructor
public class JobControlService {

    private final JobOperator jobOperator;
    private final JobExplorer jobExplorer;

    public JobExecutionStatusResponse stop(long executionId)
            throws NoSuchJobExecutionException, JobExecutionNotRunningException {
        jobOperator.stop(executionId);
        return JobExecutionStatusResponse.from(jobExplorer.getJobExecution(executionId));
    }

    public JobExecutionStatusResponse restart(long executionId)
            throws JobInstanceAlreadyCompleteException, NoSuchJobExecutionException,
            NoSuchJobException, JobRestartException, InvalidJobParametersException {
        Long newExecutionId = jobOperator.restart(executionId);
        return JobExecutionStatusResponse.from(jobExplorer.getJobExecution(newExecutionId));
    }

    public JobExecutionStatusResponse abandon(long executionId)
            throws NoSuchJobExecutionException, JobExecutionAlreadyRunningException {
        JobExecution execution = jobOperator.abandon(executionId);
        return JobExecutionStatusResponse.from(execution);
    }
}
