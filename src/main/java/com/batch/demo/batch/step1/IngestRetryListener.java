package com.batch.demo.batch.step1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.stereotype.Component;

/**
 * Makes retries in {@code ingestLineItemsStep} visible in the console, the same way
 * {@code InvoiceWriteRetryListener} does for {@code buildInvoicesStep}.
 */
@Component
public class IngestRetryListener implements RetryListener {

    private static final Logger log = LoggerFactory.getLogger(IngestRetryListener.class);

    @Override
    public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback,
                                                   Throwable throwable) {
        log.warn("Retrying ingestLineItemsStep chunk write (attempt {}) after: {}",
                context.getRetryCount(), throwable.getMessage());
    }
}
