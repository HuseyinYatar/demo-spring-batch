package com.batch.demo.batch.listener;

import java.time.Instant;

import org.springframework.batch.core.listener.SkipListener;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

import com.batch.demo.batch.dto.OrderLineCsvRecord;
import com.batch.demo.batch.reject.RejectedRecord;
import com.batch.demo.batch.reject.RejectedRecordSink;
import com.batch.demo.batch.validation.InvalidOrderLineException;
import com.batch.demo.domain.OrderLineItemStaging;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class RejectedRecordSkipListener implements SkipListener<OrderLineCsvRecord, OrderLineItemStaging> {

    private final RejectedRecordSink sink;

    @Override
    public void onSkipInRead(Throwable t) {
        String rawLine = t instanceof FlatFileParseException parseException
                ? parseException.getInput()
                : t.getMessage();
        sink.accept(new RejectedRecord("READ", rawLine, t.getMessage(), Instant.now()));
    }

    @Override
    public void onSkipInProcess(OrderLineCsvRecord item, Throwable t) {
        String reason = t instanceof InvalidOrderLineException invalidOrderLineException
                ? invalidOrderLineException.getReason()
                : t.getMessage();
        sink.accept(new RejectedRecord("PROCESS", String.valueOf(item), reason, Instant.now()));
    }
}
