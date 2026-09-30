package com.batch.demo.testsupport;

import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Each concrete subclass gets its own, independently-started container (POSTGRES is a
 * static field on the abstract class, so per-subclass, not shared) - a fresh, disposable
 * Postgres per test class, isolated from the manually-managed docker-compose instance
 * DemoApplicationTests depends on and from every other test class.
 * <p>
 * {@code @DirtiesContext} is required because of that: Spring caches contexts by
 * configuration, so two classes with identical config would otherwise share one cached
 * context still bound to the first class's (by then stopped) container, failing with
 * "Connection refused". Closing the context after each class keeps them from being reused.
 */
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class AbstractPostgresIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
}
