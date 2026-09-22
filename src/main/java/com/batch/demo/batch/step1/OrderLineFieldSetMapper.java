package com.batch.demo.batch.step1;

import java.math.BigDecimal;
import java.time.LocalDate;

import org.springframework.batch.infrastructure.item.file.mapping.FieldSetMapper;
import org.springframework.batch.infrastructure.item.file.transform.FieldSet;
import org.springframework.stereotype.Component;
import org.springframework.validation.BindException;

import com.batch.demo.batch.dto.OrderLineCsvRecord;

/**
 * Any exception thrown here (bad number/date format, wrong column count from the
 * tokenizer) is wrapped by {@code FlatFileItemReader} into a {@code FlatFileParseException},
 * which the ingest step registers as a skippable, read-level exception.
 */
@Component
public class OrderLineFieldSetMapper implements FieldSetMapper<OrderLineCsvRecord> {

    @Override
    public OrderLineCsvRecord mapFieldSet(FieldSet fieldSet) throws BindException {
        OrderLineCsvRecord record = new OrderLineCsvRecord();
        record.setOrderId(fieldSet.readString("orderId"));
        record.setCustomerId(fieldSet.readString("customerId"));
        record.setCustomerName(fieldSet.readString("customerName"));
        record.setProductId(fieldSet.readString("productId"));
        record.setProductName(fieldSet.readString("productName"));
        record.setQuantity(fieldSet.readInt("quantity"));
        record.setUnitPrice(new BigDecimal(fieldSet.readString("unitPrice")));
        record.setOrderDate(LocalDate.parse(fieldSet.readString("orderDate")));
        return record;
    }
}
