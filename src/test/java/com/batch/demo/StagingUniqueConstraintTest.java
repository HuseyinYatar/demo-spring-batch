package com.batch.demo;

import java.math.BigDecimal;
import java.time.LocalDate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;

import com.batch.demo.batch.step1.StagingInsertIgnoreDuplicatesWriter;
import com.batch.demo.domain.OrderLineItemStaging;
import com.batch.demo.repository.OrderLineItemStagingRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The DB-level unique constraint on (order_id, product_id) is the authoritative duplicate
 * guard for parallel ingestion; the processor's existsBy check alone is check-then-act.
 */
@SpringBootTest
@ActiveProfiles("test")
class StagingUniqueConstraintTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private StagingInsertIgnoreDuplicatesWriter writer;

    @Autowired
    private OrderLineItemStagingRepository repository;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void writerDropsDuplicateKeyInsteadOfFailingTheChunk() {
        // Both rows share (orderId, productId) - as if two partitions both passed the
        // processor's existsBy check before either committed.
        transactionTemplate.executeWithoutResult(status ->
                writer.write(Chunk.of(row("ORD-1", "P-1", 1), row("ORD-1", "P-1", 5), row("ORD-1", "P-2", 1))));

        assertThat(repository.count()).isEqualTo(2);
        // First writer wins, consistent with the documented "original values win" rule.
        assertThat(repository.findByOrderIdAndProcessedFalse("ORD-1"))
                .filteredOn(r -> r.getProductId().equals("P-1"))
                .singleElement()
                .satisfies(r -> assertThat(r.getQuantity()).isEqualTo(1));
    }

    @Test
    void constraintRejectsDuplicateFromAnyOtherInsertPath() {
        repository.saveAndFlush(row("ORD-2", "P-1", 1));

        assertThatThrownBy(() -> repository.saveAndFlush(row("ORD-2", "P-1", 2)))
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    }

    private static OrderLineItemStaging row(String orderId, String productId, int quantity) {
        return OrderLineItemStaging.builder()
                .orderId(orderId)
                .customerId("C-1")
                .customerName("Alice")
                .productId(productId)
                .productName("Widget")
                .quantity(quantity)
                .unitPrice(new BigDecimal("9.99"))
                .orderDate(LocalDate.now())
                .build();
    }
}
