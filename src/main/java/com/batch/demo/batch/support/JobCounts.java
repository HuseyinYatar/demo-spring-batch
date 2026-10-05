package com.batch.demo.batch.support;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.step.StepExecution;

/**
 * The job-level item counts of one {@link JobExecution}: the sum over its step
 * executions, counting each unit of work once.
 *
 * Worker step executions of a partitioned step are named "<workerStep>:<partitionKey>"
 * and are skipped, because the manager step's StepExecution already carries the sum of
 * its workers' counts (Spring Batch aggregates them on completion) - adding the workers
 * too would count every partitioned item twice.
 *
 * A JobStep's StepExecution is counted like any other: it carries its nested job's
 * totals once NestedJobCountsRollupListener has copied them on, which is also how
 * nesting stays correct at any depth.
 */
public record JobCounts(long readCount,
                        long writeCount,
                        long filterCount,
                        long readSkipCount,
                        long writeSkipCount,
                        long processSkipCount) {

    private static final String PARTITION_NAME_MARKER = ":partition";

    public static JobCounts of(JobExecution execution) {
        long read = 0;
        long write = 0;
        long filter = 0;
        long readSkip = 0;
        long writeSkip = 0;
        long processSkip = 0;
        for (StepExecution stepExecution : execution.getStepExecutions()) {
            if (stepExecution.getStepName().contains(PARTITION_NAME_MARKER)) {
                continue;
            }
            read += stepExecution.getReadCount();
            write += stepExecution.getWriteCount();
            filter += stepExecution.getFilterCount();
            readSkip += stepExecution.getReadSkipCount();
            writeSkip += stepExecution.getWriteSkipCount();
            processSkip += stepExecution.getProcessSkipCount();
        }
        return new JobCounts(read, write, filter, readSkip, writeSkip, processSkip);
    }

    public long skipCount() {
        return readSkipCount + writeSkipCount + processSkipCount;
    }

    /** Overwrites the item counts of {@code stepExecution}; commit/rollback counts describe its own transactions and stay as they are. */
    public void applyTo(StepExecution stepExecution) {
        stepExecution.setReadCount(readCount);
        stepExecution.setWriteCount(writeCount);
        stepExecution.setFilterCount(filterCount);
        stepExecution.setReadSkipCount(readSkipCount);
        stepExecution.setWriteSkipCount(writeSkipCount);
        stepExecution.setProcessSkipCount(processSkipCount);
    }
}
