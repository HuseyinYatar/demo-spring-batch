package com.batch.demo.batch.validation;

import com.batch.demo.batch.dto.OrderLineCsvRecord;

public interface OrderLineValidator {

    void validate(OrderLineCsvRecord record) throws InvalidOrderLineException;
}
