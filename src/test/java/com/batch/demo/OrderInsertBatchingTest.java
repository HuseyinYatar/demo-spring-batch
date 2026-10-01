package com.batch.demo;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;

import com.batch.demo.batch.dto.OrderInvoiceResult;
import com.batch.demo.batch.step2.OrderPersistenceItemWriter;
import com.batch.demo.domain.Invoice;
import com.batch.demo.domain.Order;
import com.batch.demo.domain.OrderLineItem;
import com.batch.demo.domain.OrderStatus;
import com.batch.demo.repository.OrderRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;

import jakarta.persistence.EntityManagerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Hibernate silently disables JDBC insert batching for IDENTITY ids, so switching a
 * generator back (or dropping hibernate.jdbc.batch_size) would not fail anything
 * functionally - it would just quietly return to one round trip per row. This pins it.
 */
@SpringBootTest
@ActiveProfiles("test")
class OrderInsertBatchingTest extends AbstractPostgresIntegrationTest {

    private static final int ORDERS = 20;

    @Autowired
    private OrderPersistenceItemWriter writer;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void ordersLineItemsAndInvoicesAreInsertedInJdbcBatches() {
        Statistics statistics = entityManagerFactory.unwrap(SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true);
        statistics.clear();

        List<OrderInvoiceResult> results = new ArrayList<>();
        for (int i = 0; i < ORDERS; i++) {
            results.add(new OrderInvoiceResult(order("ORD-B" + i), List.of()));
        }
        transactionTemplate.executeWithoutResult(status -> writer.write(new Chunk<>(results)));

        // 20 orders + 20 line items + 20 invoices.
        assertThat(statistics.getEntityInsertCount()).isEqualTo(ORDERS * 3L);
        assertThat(orderRepository.count()).isEqualTo(ORDERS);
        // Unbatched, every row is its own prepared statement (>= 60). Batched, it is one
        // per table plus a handful of sequence fetches.
        assertThat(statistics.getPrepareStatementCount()).isLessThan(ORDERS);
    }

    private static Order order(String orderNumber) {
        Order order = Order.builder()
                .orderNumber(orderNumber)
                .customerId("C-1")
                .customerName("Alice")
                .orderDate(LocalDate.now())
                .status(OrderStatus.values()[0])
                .build();
        order.addLineItem(OrderLineItem.builder()
                .productId("P-1")
                .productName("Widget")
                .quantity(1)
                .unitPrice(new BigDecimal("10.00"))
                .lineTotal(new BigDecimal("10.00"))
                .build());
        order.setInvoice(Invoice.builder()
                .order(order)
                .invoiceNumber("INV-" + orderNumber)
                .subtotal(new BigDecimal("10.00"))
                .taxAmount(new BigDecimal("1.80"))
                .totalAmount(new BigDecimal("11.80"))
                .issuedDate(LocalDate.now())
                .build());
        return order;
    }
}
