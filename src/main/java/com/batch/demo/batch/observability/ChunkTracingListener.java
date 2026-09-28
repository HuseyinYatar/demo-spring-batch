package com.batch.demo.batch.observability;

import org.springframework.batch.core.listener.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

import lombok.RequiredArgsConstructor;

/**
 * Spring Batch's own Micrometer instrumentation (see BatchJobConfig/CLAUDE.md) only
 * covers job and step executions, so a partitioned worker step's trace shows one long
 * span per partition with no visibility into how many chunks it took or which chunk was
 * slow or retried. This gives each chunk commit its own "batch.chunk" span, nested
 * under the worker step's span - which is itself nested under the job span as long as
 * batchTaskExecutor's ContextPropagatingTaskDecorator carried the trace context onto
 * the partition's worker thread (see BatchJobConfig).
 *
 * Registered on both worker steps (identical behaviour for each), so it's a shared
 * singleton rather than duplicated per step.
 */
@Component
@RequiredArgsConstructor
public class ChunkTracingListener implements ChunkListener<Object, Object> {

    private static final String OBSERVATION_ATTR = "chunkTracingObservation";
    private static final String SCOPE_ATTR = "chunkTracingScope";

    private final ObservationRegistry observationRegistry;

    @Override
    public void beforeChunk(ChunkContext context) {
        StepExecution stepExecution = context.getStepContext().getStepExecution();
        Observation observation = Observation.createNotStarted("batch.chunk", observationRegistry)
                .lowCardinalityKeyValue("spring.batch.chunk.step.name", stepExecution.getStepName())
                .lowCardinalityKeyValue("spring.batch.chunk.job.name",
                        stepExecution.getJobExecution().getJobInstance().getJobName())
                // Chunk index is unbounded over a job's lifetime, so it stays out of the
                // low-cardinality tags a meter would be keyed by - it's only visible on
                // the span itself.
                .highCardinalityKeyValue("spring.batch.chunk.commit.count", String.valueOf(stepExecution.getCommitCount()))
                .start();
        context.setAttribute(OBSERVATION_ATTR, observation);
        context.setAttribute(SCOPE_ATTR, observation.openScope());
    }

    @Override
    public void afterChunk(ChunkContext context) {
        stop(context, null);
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        Throwable error = (Throwable) context.getAttribute(ChunkListener.ROLLBACK_EXCEPTION_KEY);
        stop(context, error);
    }

    private void stop(ChunkContext context, Throwable error) {
        Observation.Scope scope = (Observation.Scope) context.removeAttribute(SCOPE_ATTR);
        Observation observation = (Observation) context.removeAttribute(OBSERVATION_ATTR);
        if (observation == null) {
            return;
        }
        scope.close();
        if (error != null) {
            observation.error(error);
        }
        observation.stop();
    }
}
