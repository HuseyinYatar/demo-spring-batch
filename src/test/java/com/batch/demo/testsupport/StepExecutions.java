package com.batch.demo.testsupport;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.step.StepExecution;

/**
 * A StepExecution-returning method declared directly on a @SpringBatchTest test class
 * gets misdetected by StepScopeTestExecutionListener (which scans the test class for a
 * step-execution-provider method by return type, not by name) - kept as a separate
 * utility class specifically to avoid that.
 */
public final class StepExecutions {

    private StepExecutions() {
    }

    public static StepExecution named(JobExecution execution, String stepName) {
        return execution.getStepExecutions().stream()
                .filter(step -> step.getStepName().equals(stepName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No step execution named " + stepName));
    }
}
