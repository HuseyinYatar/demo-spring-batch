package com.batch.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.observation.ServerRequestObservationContext;

import io.micrometer.observation.ObservationPredicate;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Boot's ServerHttpObservationFilter observes every HTTP request with no built-in
 * exclusion for actuator paths - without this, Prometheus's own 15s scrape of
 * /actuator/prometheus (and any health check polling) would each mint their own
 * throwaway trace in Tempo, drowning out the one trace per job run that's actually
 * worth looking at.
 */
@Configuration
public class ObservabilityConfig {

    @Bean
    public ObservationPredicate noActuatorHttpObservations() {
        return (name, context) -> {
            if (context instanceof ServerRequestObservationContext serverContext) {
                HttpServletRequest request = serverContext.getCarrier();
                return !request.getRequestURI().startsWith("/actuator");
            }
            return true;
        };
    }
}
