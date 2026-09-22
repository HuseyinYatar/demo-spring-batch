package com.batch.demo.batch.reject;

public interface RejectedRecordSink {

    void accept(RejectedRecord rejectedRecord);

    void reset();
}
