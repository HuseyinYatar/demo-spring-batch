package com.batch.demo.batch.validation.rules;

import java.util.Optional;

import org.springframework.stereotype.Component;

import com.batch.demo.batch.dto.OrderLineCsvRecord;
import com.batch.demo.batch.validation.LineItemValidationRule;

@Component
public class PositiveQuantityRule implements LineItemValidationRule {

    @Override
    public Optional<String> validate(OrderLineCsvRecord record) {
        if (record.getQuantity() == null || record.getQuantity() <= 0) {
            return Optional.of("quantity must be a positive number");
        }
        return Optional.empty();
    }
}
