package com.batch.demo.batch.validation;

import java.util.Optional;

import com.batch.demo.batch.dto.OrderLineCsvRecord;

public interface LineItemValidationRule {

    Optional<String> validate(OrderLineCsvRecord record);
}
