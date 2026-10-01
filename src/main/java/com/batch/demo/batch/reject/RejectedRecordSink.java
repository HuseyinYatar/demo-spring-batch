package com.batch.demo.batch.reject;

import java.time.LocalDate;

/**
 * Rejects are kept per business date (one file per run date), so every call names the
 * date it belongs to.
 */
public interface RejectedRecordSink {

    void accept(LocalDate businessDate, RejectedRecord rejectedRecord);

    void reset(LocalDate businessDate);
}
