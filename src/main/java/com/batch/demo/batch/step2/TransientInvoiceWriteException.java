package com.batch.demo.batch.step2;

/**
 * Registered as a retryable exception on {@code buildInvoicesStep} - represents a
 * transient failure persisting an invoice (e.g. a dropped DB connection), as opposed
 * to a permanent, data-driven failure that skip handles in step 1.
 */
public class TransientInvoiceWriteException extends RuntimeException {

    public TransientInvoiceWriteException(String message) {
        super(message);
    }
}
