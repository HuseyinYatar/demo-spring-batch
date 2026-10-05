package com.batch.demo.web.dto;

import java.time.LocalDateTime;

import org.springframework.batch.core.job.JobExecution;

import com.batch.demo.batch.support.JobCounts;

public record JobExecutionStatusResponse(Long jobExecutionId,
                                          String jobName,
                                          String status,
                                          String exitCode,
                                          LocalDateTime startTime,
                                          LocalDateTime endTime,
                                          long readCount,
                                          long writeCount,
                                          long skipCount) {

    public static JobExecutionStatusResponse from(JobExecution execution) {
        JobCounts counts = JobCounts.of(execution);

        return new JobExecutionStatusResponse(
                execution.getId(),
                execution.getJobInstance().getJobName(),
                execution.getStatus().name(),
                execution.getExitStatus().getExitCode(),
                execution.getStartTime(),
                execution.getEndTime(),
                counts.readCount(),
                counts.writeCount(),
                counts.skipCount());
    }
}
