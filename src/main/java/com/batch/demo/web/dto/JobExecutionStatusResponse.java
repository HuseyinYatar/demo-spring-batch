package com.batch.demo.web.dto;

import java.time.LocalDateTime;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.step.StepExecution;

public record JobExecutionStatusResponse(Long jobExecutionId,
                                          String jobName,
                                          String status,
                                          String exitCode,
                                          LocalDateTime startTime,
                                          LocalDateTime endTime,
                                          long readCount,
                                          long writeCount,
                                          long skipCount) {

    // Worker step executions of a partitioned step are named "<workerStep>:<partitionKey>".
    private static final String PARTITION_NAME_MARKER = ":partition";

    public static JobExecutionStatusResponse from(JobExecution execution) {
        long readCount = 0;
        long writeCount = 0;
        long skipCount = 0;
        for (StepExecution stepExecution : execution.getStepExecutions()) {
            // The manager step's StepExecution already carries the sum of its workers'
            // counts (Spring Batch aggregates them on completion), so adding the worker
            // executions too would count every partitioned item twice.
            if (stepExecution.getStepName().contains(PARTITION_NAME_MARKER)) {
                continue;
            }
            readCount += stepExecution.getReadCount();
            writeCount += stepExecution.getWriteCount();
            skipCount += stepExecution.getSkipCount();
        }

        return new JobExecutionStatusResponse(
                execution.getId(),
                execution.getJobInstance().getJobName(),
                execution.getStatus().name(),
                execution.getExitStatus().getExitCode(),
                execution.getStartTime(),
                execution.getEndTime(),
                readCount,
                writeCount,
                skipCount);
    }
}
