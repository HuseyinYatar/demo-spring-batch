package com.batch.demo.testsupport.fault;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Deterministic, one-shot "let N matching inserts through, then fail the next one"
 * trigger - same latch style as the production FlakyOrderPersistenceSimulator, but
 * test-only and armed explicitly per test rather than auto-resetting per job run.
 */
public class OrderInsertFaultTrigger {

    private final AtomicInteger remainingSuccesses = new AtomicInteger(-1);
    private final AtomicBoolean fired = new AtomicBoolean(false);

    public void arm(int allowedSuccessesBeforeFailure) {
        fired.set(false);
        remainingSuccesses.set(allowedSuccessesBeforeFailure);
    }

    public void disarm() {
        remainingSuccesses.set(-1);
    }

    boolean shouldFailFor(String sql) {
        if (remainingSuccesses.get() < 0 || fired.get() || sql == null) {
            return false;
        }
        if (!sql.trim().toLowerCase(Locale.ROOT).contains("insert into orders")) {
            return false;
        }
        return remainingSuccesses.getAndDecrement() == 0 && fired.compareAndSet(false, true);
    }
}
