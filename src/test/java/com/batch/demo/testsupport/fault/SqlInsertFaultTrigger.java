package com.batch.demo.testsupport.fault;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Deterministic, one-shot "let N matching inserts through, then fail the next one"
 * trigger, parametrized by the SQL fragment to match (e.g. "insert into orders" or
 * "insert into order_line_item_staging") - the fault-injection scenarios against
 * different tables need identical logic, just a different match target, so this one
 * class serves both rather than duplicating the trigger per table.
 *
 * <p>Can optionally simulate a genuine connection failure instead of a generic
 * SQLException - throwing the standard {@link SQLTransientConnectionException} JDBC
 * type is what Hibernate's exception classification recognizes by type (not by
 * fuzzy SQLState-string parsing) and translates into
 * {@code org.hibernate.exception.JDBCConnectionException}, which Spring in turn maps
 * to {@code org.springframework.dao.DataAccessResourceFailureException} - the real
 * exception type the retry policies now also register (see
 * BuildInvoicesStepConfig/IngestLineItemsStepConfig).
 */
public class SqlInsertFaultTrigger {

    private final String matchedSqlFragment;
    private final boolean simulateConnectionFailure;
    private final AtomicInteger remainingSuccesses = new AtomicInteger(-1);
    private final AtomicBoolean fired = new AtomicBoolean(false);

    public SqlInsertFaultTrigger(String matchedSqlFragment) {
        this(matchedSqlFragment, false);
    }

    public SqlInsertFaultTrigger(String matchedSqlFragment, boolean simulateConnectionFailure) {
        this.matchedSqlFragment = matchedSqlFragment.toLowerCase(Locale.ROOT);
        this.simulateConnectionFailure = simulateConnectionFailure;
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

    SQLException buildException() {
        String message = "Injected fault: simulated failure preparing statement";
        return simulateConnectionFailure ? new SQLTransientConnectionException(message) : new SQLException(message);
    }
}
