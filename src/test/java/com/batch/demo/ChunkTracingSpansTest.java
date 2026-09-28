package com.batch.demo;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;

import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies ChunkTracingListener actually produces a "batch.chunk" span per chunk, and -
 * the real hazard being tested - that those spans nest under their own partition's
 * worker-step trace despite running on batchTaskExecutor's own threads. Without
 * BatchJobConfig's ContextPropagatingTaskDecorator, each partition would start its own
 * disconnected trace instead of a child of the manager step/job trace.
 *
 * CapturingSpanExporter receives every span the OTel SDK's BatchSpanProcessor flushes,
 * regardless of whether a real Tempo instance is reachable, so this test needs no
 * external tracing backend running - it just needs to force a flush itself, since the
 * batch processor otherwise only exports on its own schedule delay.
 */
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = {
        "batch.input-csv-path=classpath:data/test-order-line-items-partitioned.csv",
        "batch.partition-grid-size=3"
})
@SpringBatchTest
class ChunkTracingSpansTest extends AbstractPostgresIntegrationTest {

    @TestConfiguration
    static class CapturingSpanExporterConfig {
        @Bean
        CapturingSpanExporter capturingSpanExporter() {
            return new CapturingSpanExporter();
        }
    }

    static class CapturingSpanExporter implements SpanExporter {
        final List<SpanData> spans = new CopyOnWriteArrayList<>();

        @Override
        public CompletableResultCode export(Collection<SpanData> spans) {
            this.spans.addAll(spans);
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }
    }

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private CapturingSpanExporter capturingSpanExporter;

    @Autowired
    private SdkTracerProvider sdkTracerProvider;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
        capturingSpanExporter.spans.clear();
    }

    @Test
    void chunkSpansAreCapturedAndNestUnderEveryPartition() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("businessDate", LocalDate.now())
                .addString("inputFile", "classpath:data/test-order-line-items-partitioned.csv")
                .toJobParameters();

        JobExecution execution = jobLauncherTestUtils.launchJob(jobParameters);
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        // The BatchSpanProcessor exports on its own schedule delay (default 5s), not
        // immediately on span.stop() - force it so the assertions below don't race it.
        sdkTracerProvider.forceFlush().join(10, TimeUnit.SECONDS);

        List<SpanData> chunkSpans = capturingSpanExporter.spans.stream()
                .filter(span -> "batch.chunk".equals(span.getName()))
                .toList();
        assertThat(chunkSpans).isNotEmpty();

        // Every partition of both worker steps must have produced at least one chunk
        // span - proves the trace context actually reached batchTaskExecutor's worker
        // threads, not just the manager thread that submitted them.
        AttributeKey<String> stepNameKey = AttributeKey.stringKey("spring.batch.chunk.step.name");
        Set<String> stepNamesWithChunkSpans = chunkSpans.stream()
                .map(span -> span.getAttributes().get(stepNameKey))
                .collect(Collectors.toSet());
        for (int partition = 0; partition < 3; partition++) {
            assertThat(stepNamesWithChunkSpans).contains("ingestLineItemsWorkerStep:partition" + partition);
            assertThat(stepNamesWithChunkSpans).contains("buildInvoicesWorkerStep:partition" + partition);
        }

        // And they must all share one trace - i.e. actually nested under the job's
        // trace, not orphaned new traces independently started on each worker thread.
        Set<String> traceIds = chunkSpans.stream().map(SpanData::getTraceId).collect(Collectors.toSet());
        assertThat(traceIds).hasSize(1);
    }
}
