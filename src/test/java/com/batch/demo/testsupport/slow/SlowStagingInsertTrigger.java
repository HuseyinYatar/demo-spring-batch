package com.batch.demo.testsupport.slow;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Slows down every "insert into order_line_item_staging" statement while armed, so a
 * test has a reliable window to observe a job in a genuinely STARTED state before it
 * finishes - real jobs against these tiny fixtures otherwise complete in well under
 * the time a test needs to launch it asynchronously and poll for its status.
 */
public class SlowStagingInsertTrigger {

    private volatile long delayMillis;
    private final AtomicBoolean enabled = new AtomicBoolean(false);

    public void arm(long delayMillis) {
        this.delayMillis = delayMillis;
        enabled.set(true);
    }

    public void disarm() {
        enabled.set(false);
    }

    long delayMillisFor(String sql) {
        if (!enabled.get() || sql == null) {
            return 0;
        }
        return sql.trim().toLowerCase(Locale.ROOT).contains("insert into order_line_item_staging") ? delayMillis : 0;
    }
}
