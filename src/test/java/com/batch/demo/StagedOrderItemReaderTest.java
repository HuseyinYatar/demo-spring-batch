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
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import com.batch.demo.batch.dto.OrderInvoiceResult;
import com.batch.demo.batch.dto.StagedOrder;
import com.batch.demo.batch.step2.StagedOrderItemReader;
import com.batch.demo.batch.step2.StagingMarkProcessedItemWriter;
import com.batch.demo.domain.Order;
import com.batch.demo.domain.OrderLineItemStaging;
import com.batch.demo.domain.OrderStatus;
import com.batch.demo.repository.OrderLineItemStagingRepository;
import com.batch.demo.repository.OrderRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;

import jakarta.persistence.EntityManagerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * The invoice step used to run two queries per order (already invoiced? + its lines) from
 * the processor. The reader now does both for a whole page of orders, so the number of
 * queries follows the page count, not the order count - nothing functional breaks if that
 * quietly regresses to per-order lookups, so it is pinned here.
 */
@SpringBootTest
@ActiveProfiles("test")
class StagedOrderItemReaderTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private OrderLineItemStagingRepository stagingRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private StagingMarkProcessedItemWriter markProcessedWriter;

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void queryCountFollowsThePageCountNotTheOrderCount() {
        for (int i = 1; i <= 10; i++) {
            stagingRepository.save(line(orderId(i), "P-1", false));
            stagingRepository.save(line(orderId(i), "P-2", false));
        }
        Statistics statistics = entityManagerFactory.unwrap(SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true);
        statistics.clear();

        List<StagedOrder> orders = readAll(reader(orderId(1), orderId(10), 4));

        assertThat(orders).extracting(StagedOrder::orderId)
                .containsExactly(orderId(1), orderId(2), orderId(3), orderId(4), orderId(5),
                        orderId(6), orderId(7), orderId(8), orderId(9), orderId(10));
        assertThat(orders).allSatisfy(order -> assertThat(order.lines()).hasSize(2));
        // 10 orders at 4 per page = 3 pages, each: id page + already-invoiced check + lines.
        // Per-order lookups would be 20+.
        assertThat(statistics.getPrepareStatementCount()).isEqualTo(9);
    }

    @Test
    void skipsAlreadyInvoicedOrdersAndKeepsPagingPastAPageOfThem() {
        for (int i = 1; i <= 6; i++) {
            stagingRepository.save(line(orderId(i), "P-1", false));
        }
        // Orders 1-4 fill the first two pages (size 2) completely, so those pages are empty
        // once filtered and the reader has to go on to the third on its own.
        for (int i = 1; i <= 4; i++) {
            orderRepository.save(Order.builder().orderNumber(orderId(i)).status(OrderStatus.INVOICED).build());
        }

        List<StagedOrder> orders = readAll(reader(orderId(1), orderId(6), 2));

        assertThat(orders).extracting(StagedOrder::orderId).containsExactly(orderId(5), orderId(6));
    }

    @Test
    void returnsOnlyUnprocessedLinesInStagingOrder() {
        // Staged P-2 before P-1, so staging order and product order differ.
        stagingRepository.save(line("ORD-A", "P-2", false));
        stagingRepository.save(line("ORD-A", "P-1", false));
        stagingRepository.save(line("ORD-A", "P-3", true));
        stagingRepository.save(line("ORD-B", "P-1", true));

        List<StagedOrder> orders = readAll(reader("ORD-A", "ORD-B", 10));

        assertThat(orders).singleElement().satisfies(order -> {
            assertThat(order.orderId()).isEqualTo("ORD-A");
            assertThat(order.lines()).extracting(OrderLineItemStaging::getProductId).containsExactly("P-2", "P-1");
        });
    }

    @Test
    void readsNothingWhenThePartitionHasNoRange() {
        stagingRepository.save(line(orderId(1), "P-1", false));

        assertThat(readAll(reader(null, null, 10))).isEmpty();
    }

    @Test
    void markProcessedWriterUpdatesOnlyTheGivenRowsAsDetachedEntities() {
        OrderLineItemStaging first = stagingRepository.save(line("ORD-A", "P-1", false));
        OrderLineItemStaging second = stagingRepository.save(line("ORD-B", "P-1", false));
        stagingRepository.save(line("ORD-C", "P-1", false));

        markProcessedWriter.write(Chunk.of(
                new OrderInvoiceResult(null, List.of(first)),
                new OrderInvoiceResult(null, List.of(second))));

        assertThat(stagingRepository.findAll())
                .extracting(OrderLineItemStaging::getOrderId, OrderLineItemStaging::isProcessed)
                .containsExactlyInAnyOrder(tuple("ORD-A", true), tuple("ORD-B", true), tuple("ORD-C", false));
    }

    private StagedOrderItemReader reader(String from, String to, int pageSize) {
        StagedOrderItemReader reader = new StagedOrderItemReader(stagingRepository, orderRepository, from, to, pageSize);
        reader.open(new ExecutionContext());
        return reader;
    }

    private static List<StagedOrder> readAll(StagedOrderItemReader reader) {
        List<StagedOrder> orders = new ArrayList<>();
        try {
            for (StagedOrder order = reader.read(); order != null; order = reader.read()) {
                orders.add(order);
            }
        } finally {
            reader.close();
        }
        return orders;
    }

    private static String orderId(int i) {
        return String.format("ORD-%06d", i);
    }

    private static OrderLineItemStaging line(String orderId, String productId, boolean processed) {
        return OrderLineItemStaging.builder()
                .orderId(orderId)
                .customerId("C-1")
                .customerName("Alice")
                .productId(productId)
                .productName("Widget")
                .quantity(1)
                .unitPrice(new BigDecimal("9.99"))
                .orderDate(LocalDate.now())
                .processed(processed)
                .build();
    }
}
