package com.batch.demo;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import com.batch.demo.batch.step2.OrderIdRangePartitioner;
import com.batch.demo.domain.OrderLineItemStaging;
import com.batch.demo.repository.OrderLineItemStagingRepository;
import com.batch.demo.testsupport.AbstractPostgresIntegrationTest;
import com.batch.demo.testsupport.BusinessDataCleaner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The partitioner asks the database (ntile) for range boundaries instead of loading every
 * order id; the ranges must still cover each unprocessed order exactly once.
 *
 * The @TestPropertySource is deliberately unique to this class: AbstractPostgresIntegrationTest
 * restarts its container on a new port per test class, so two classes with an identical context
 * config would share a cached context still pointing at the previous class's dead container.
 */
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = "batch.order-id-page-size=7")
class OrderIdRangePartitionerTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private OrderLineItemStagingRepository repository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        BusinessDataCleaner.truncateAll(jdbcTemplate);
    }

    @Test
    void rangesCoverEveryUnprocessedOrderExactlyOnce() {
        // 10 orders, two lines each, so distinct-ing matters; 3 partitions -> buckets of 4/3/3.
        for (int i = 1; i <= 10; i++) {
            repository.save(row(orderId(i), "P-1", false));
            repository.save(row(orderId(i), "P-2", false));
        }

        Map<String, ExecutionContext> partitions = new OrderIdRangePartitioner(repository).partition(3);

        assertThat(partitions).hasSize(3);
        List<String> covered = new ArrayList<>();
        for (int p = 0; p < 3; p++) {
            ExecutionContext ctx = partitions.get("partition" + p);
            assertThat(ctx.getString("partitionKey")).isEqualTo("partition" + p);
            for (int i = 1; i <= 10; i++) {
                String id = orderId(i);
                if (id.compareTo(ctx.getString("fromOrderId")) >= 0 && id.compareTo(ctx.getString("toOrderId")) <= 0) {
                    covered.add(id);
                }
            }
        }
        assertThat(covered).hasSize(10).doesNotHaveDuplicates();
        assertThat(partitions.get("partition0").getString("fromOrderId")).isEqualTo("ORD-000001");
        assertThat(partitions.get("partition2").getString("toOrderId")).isEqualTo("ORD-000010");
    }

    @Test
    void ignoresProcessedOrders() {
        repository.save(row("ORD-000001", "P-1", true));
        repository.save(row("ORD-000002", "P-1", false));

        Map<String, ExecutionContext> partitions = new OrderIdRangePartitioner(repository).partition(2);

        ExecutionContext first = partitions.get("partition0");
        assertThat(first.getString("fromOrderId")).isEqualTo("ORD-000002");
        assertThat(first.getString("toOrderId")).isEqualTo("ORD-000002");
    }

    @Test
    void fewerOrdersThanPartitionsLeavesTrailingPartitionsEmpty() {
        repository.save(row("ORD-000001", "P-1", false));
        repository.save(row("ORD-000002", "P-1", false));

        Map<String, ExecutionContext> partitions = new OrderIdRangePartitioner(repository).partition(4);

        assertThat(partitions).hasSize(4);
        assertThat(partitions.get("partition0").containsKey("fromOrderId")).isTrue();
        assertThat(partitions.get("partition1").containsKey("fromOrderId")).isTrue();
        assertThat(partitions.get("partition2").containsKey("fromOrderId")).isFalse();
        assertThat(partitions.get("partition3").containsKey("fromOrderId")).isFalse();
    }

    @Test
    void noUnprocessedOrdersStillEmitsEveryPartitionEmpty() {
        Map<String, ExecutionContext> partitions = new OrderIdRangePartitioner(repository).partition(3);

        assertThat(partitions).hasSize(3);
        assertThat(partitions.values()).noneMatch(ctx -> ctx.containsKey("fromOrderId"));
    }

    private static String orderId(int i) {
        return String.format("ORD-%06d", i);
    }

    private static OrderLineItemStaging row(String orderId, String productId, boolean processed) {
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
