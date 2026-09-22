package com.batch.demo.batch.step2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.stereotype.Component;

/**
 * Makes retries in {@code buildInvoicesStep} visible in the console, the same way
 * {@code RejectedRecordSkipListener} makes skips visible in rejected-rows.csv.
 */
@Component
public class InvoiceWriteRetryListener implements RetryListener {

    private static final Logger log = LoggerFactory.getLogger(InvoiceWriteRetryListener.class);

    @Override
    public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback,
                                                   Throwable throwable) {
        log.warn("Retrying buildInvoicesStep chunk write (attempt {}) after: {}",
                context.getRetryCount(), throwable.getMessage());
    }
}
