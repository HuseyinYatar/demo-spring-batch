package com.batch.demo.batch.validation.rules;

import java.util.Optional;

import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.batch.demo.batch.dto.OrderLineCsvRecord;
import com.batch.demo.batch.validation.LineItemValidationRule;

@Component
public class NonBlankCustomerNameRule implements LineItemValidationRule {

    @Override
    public Optional<String> validate(OrderLineCsvRecord record) {
        if (!StringUtils.hasText(record.getCustomerName())) {
            return Optional.of("customerName must not be blank");
        }
        return Optional.empty();
    }
}
