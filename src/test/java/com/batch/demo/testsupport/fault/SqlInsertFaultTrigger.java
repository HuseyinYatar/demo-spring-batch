package com.batch.demo.testsupport.fault;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Deterministic, one-shot "let N matching inserts through, then fail the next one"
 * trigger, parametrized by the SQL fragment to match (e.g. "insert into orders" or
 * "insert into order_line_item_staging") - the fault-injection scenarios against
 * different tables need identical logic, just a different match target, so this one
 * class serves both rather than duplicating the trigger per table.
 */
public class SqlInsertFaultTrigger {

    private final String matchedSqlFragment;
    private final AtomicInteger remainingSuccesses = new AtomicInteger(-1);
    private final AtomicBoolean fired = new AtomicBoolean(false);

    public SqlInsertFaultTrigger(String matchedSqlFragment) {
        this.matchedSqlFragment = matchedSqlFragment.toLowerCase(Locale.ROOT);
    }

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
        if (!sql.trim().toLowerCase(Locale.ROOT).contains(matchedSqlFragment)) {
            return false;
        }
        return remainingSuccesses.getAndDecrement() == 0 && fired.compareAndSet(false, true);
    }
}
