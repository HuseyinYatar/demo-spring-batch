package com.batch.demo.batch.step2;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Component;

import com.batch.demo.config.BatchProperties;

/**
 * Simulates one transient failure per job run, so {@code buildInvoicesStep}'s retry
 * policy has something real to demonstrate - a healthy local Postgres never actually
 * throws a transient error on its own. {@link #reset()} is called from the job's
 * beforeJob listener so every run gets exactly one simulated failure, deterministically,
 * rather than relying on randomness.
 */
@Component
public class FlakyOrderPersistenceSimulator {

    private final boolean enabled;
    private final AtomicBoolean hasFailedOnce = new AtomicBoolean(false);

    public FlakyOrderPersistenceSimulator(BatchProperties properties) {
        this.enabled = properties.isSimulateTransientWriteFailures();
    }

    public void reset() {
        hasFailedOnce.set(false);
    }

    public void maybeFailOnce() {
        if (enabled && hasFailedOnce.compareAndSet(false, true)) {
            throw new TransientInvoiceWriteException(
                    "Simulated transient failure writing to the invoice store");
        }
    }
}
