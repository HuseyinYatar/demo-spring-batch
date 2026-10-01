package com.batch.demo.batch.listener;

import java.time.Instant;
import java.time.LocalDate;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.listener.SkipListener;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.batch.demo.batch.dto.OrderLineCsvRecord;
import com.batch.demo.batch.reject.RejectedRecord;
import com.batch.demo.batch.reject.RejectedRecordSink;
import com.batch.demo.batch.validation.InvalidOrderLineException;
import com.batch.demo.domain.OrderLineItemStaging;

import lombok.RequiredArgsConstructor;

/**
 * @StepScope so businessDate can be late-bound from the run's JobParameters (same
 * pattern as InvoiceAggregationProcessor) - a plain singleton couldn't tell which
 * date's rejects file a skipped row belongs to if runs for different dates overlap.
 */
@Component
@StepScope
@RequiredArgsConstructor
public class RejectedRecordSkipListener implements SkipListener<OrderLineCsvRecord, OrderLineItemStaging> {

    private final RejectedRecordSink sink;

    @Value("#{jobParameters['businessDate']}")
    private final LocalDate businessDate;

    @Override
    public void onSkipInRead(Throwable t) {
        String rawLine = t instanceof FlatFileParseException parseException
                ? parseException.getInput()
                : t.getMessage();
        sink.accept(businessDate, new RejectedRecord("READ", rawLine, t.getMessage(), Instant.now()));
    }

    @Override
    public void onSkipInProcess(OrderLineCsvRecord item, Throwable t) {
        String reason = t instanceof InvalidOrderLineException invalidOrderLineException
                ? invalidOrderLineException.getReason()
                : t.getMessage();
        sink.accept(businessDate, new RejectedRecord("PROCESS", String.valueOf(item), reason, Instant.now()));
    }
}
