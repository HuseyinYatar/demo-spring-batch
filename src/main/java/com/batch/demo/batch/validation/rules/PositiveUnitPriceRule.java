package com.batch.demo.batch.validation.rules;

import java.math.BigDecimal;
import java.util.Optional;

import org.springframework.stereotype.Component;

import com.batch.demo.batch.dto.OrderLineCsvRecord;
import com.batch.demo.batch.validation.LineItemValidationRule;

@Component
public class PositiveUnitPriceRule implements LineItemValidationRule {

    @Override
    public Optional<String> validate(OrderLineCsvRecord record) {
        if (record.getUnitPrice() == null || record.getUnitPrice().compareTo(BigDecimal.ZERO) <= 0) {
            return Optional.of("unitPrice must be a positive number");
        }
        return Optional.empty();
    }
}
