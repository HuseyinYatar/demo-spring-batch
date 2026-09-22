package com.batch.demo.batch.validation;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.batch.demo.batch.dto.OrderLineCsvRecord;
import com.batch.demo.batch.validation.rules.NonBlankCustomerNameRule;
import com.batch.demo.batch.validation.rules.NonBlankOrderIdRule;
import com.batch.demo.batch.validation.rules.PositiveQuantityRule;
import com.batch.demo.batch.validation.rules.PositiveUnitPriceRule;

class DefaultOrderLineValidatorTest {

    private final OrderLineValidator validator = new DefaultOrderLineValidator(List.of(
            new NonBlankOrderIdRule(),
            new NonBlankCustomerNameRule(),
            new PositiveQuantityRule(),
            new PositiveUnitPriceRule()));

    @Test
    void passesForAValidRecord() {
        OrderLineCsvRecord record = validRecord();

        assertThatCode(() -> validator.validate(record)).doesNotThrowAnyException();
    }

    @Test
    void rejectsANegativeQuantity() {
        OrderLineCsvRecord record = validRecord();
        record.setQuantity(-1);

        assertThatThrownBy(() -> validator.validate(record))
                .isInstanceOf(InvalidOrderLineException.class)
                .hasMessageContaining("quantity");
    }

    @Test
    void rejectsABlankOrderId() {
        OrderLineCsvRecord record = validRecord();
        record.setOrderId(" ");

        assertThatThrownBy(() -> validator.validate(record))
                .isInstanceOf(InvalidOrderLineException.class)
                .hasMessageContaining("orderId");
    }

    @Test
    void combinesMultipleViolationsIntoOneException() {
        OrderLineCsvRecord record = validRecord();
        record.setQuantity(-1);
        record.setUnitPrice(BigDecimal.ZERO);

        assertThatThrownBy(() -> validator.validate(record))
                .isInstanceOf(InvalidOrderLineException.class)
                .hasMessageContaining("quantity")
                .hasMessageContaining("unitPrice");
    }

    private OrderLineCsvRecord validRecord() {
        return new OrderLineCsvRecord(
                "ORD-1001", "C001", "Ananya Sharma", "P001", "Wireless Mouse",
                2, new BigDecimal("799.00"), LocalDate.of(2026, 1, 2));
    }
}
