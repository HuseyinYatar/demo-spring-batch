package com.batch.demo.batch.validation;

import lombok.Getter;

@Getter
public class InvalidOrderLineException extends RuntimeException {

    private final String orderId;
    private final String reason;

    public InvalidOrderLineException(String orderId, String reason) {
        super(reason);
        this.orderId = orderId;
        this.reason = reason;
    }
}
