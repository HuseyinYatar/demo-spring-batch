package com.batch.demo;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;

import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Verifies ObservabilityConfig's ObservationPredicate actually stops actuator requests
 * from becoming spans, without also silencing real application traffic. Without it,
 * every hit under /actuator/** - including Prometheus's own periodic scrape - would
 * mint its own throwaway trace in Tempo alongside the one trace per job run that's
 * actually worth looking at.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@ActiveProfiles("test")
@SpringBatchTest
class ActuatorObservationExclusionTest extends AbstractPostgresIntegrationTest {

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
    private MockMvc mockMvc;

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
    void actuatorRequestsProduceNoSpanButApplicationRequestsStillDo() throws Exception {
        mockMvc.perform(get("/actuator/health")).andExpect(status().isOk());
        mockMvc.perform(get("/actuator/prometheus")).andExpect(status().isOk());

        mockMvc.perform(post("/api/batch/jobs/order-processing")).andExpect(status().isOk());

        sdkTracerProvider.forceFlush().join(10, TimeUnit.SECONDS);

        assertThat(capturingSpanExporter.spans)
                .noneMatch(span -> span.getName().toLowerCase().contains("actuator"));
        assertThat(capturingSpanExporter.spans)
                .anyMatch(span -> span.getName().toLowerCase().contains("order-processing"));
    }
}
