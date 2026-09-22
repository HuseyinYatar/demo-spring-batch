package com.batch.demo.batch.validation;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.batch.demo.batch.dto.OrderLineCsvRecord;

import lombok.RequiredArgsConstructor;

/**
 * Runs every {@link LineItemValidationRule} bean against a record. New rules are added
 * by creating a new rule implementation - this class never changes (open/closed).
 */
@Component
@RequiredArgsConstructor
public class DefaultOrderLineValidator implements OrderLineValidator {

    private final List<LineItemValidationRule> rules;

    @Override
    public void validate(OrderLineCsvRecord record) throws InvalidOrderLineException {
        String violations = rules.stream()
                .map(rule -> rule.validate(record))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.joining("; "));

        if (!violations.isEmpty()) {
            throw new InvalidOrderLineException(record.getOrderId(), violations);
        }
    }
}
