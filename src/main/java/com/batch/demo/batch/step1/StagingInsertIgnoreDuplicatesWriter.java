package com.batch.demo.batch.step1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;

import com.batch.demo.domain.OrderLineItemStaging;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

/**
 * Inserts staging rows with {@code ON CONFLICT (order_id, product_id) DO NOTHING}, so the
 * {@code uk_staging_order_product} unique constraint is the authoritative duplicate guard.
 * The processor's {@code existsBy} check is only a cheap pre-filter: it is check-then-act,
 * so two concurrent writers (partitions, or overlapping job runs) can both pass it for the
 * same key. A plain INSERT would then abort the whole chunk on the loser; here the loser's
 * row is dropped instead.
 *
 * Uses the transaction-bound {@link EntityManager} directly (not JdbcTemplate) so a real
 * connection failure still surfaces as Hibernate's own JDBCConnectionException, which
 * ingestLineItemsWorkerStep's retry policy is registered for.
 */
public class StagingInsertIgnoreDuplicatesWriter implements ItemWriter<OrderLineItemStaging> {

    private static final Logger log = LoggerFactory.getLogger(StagingInsertIgnoreDuplicatesWriter.class);

    private static final String INSERT_SQL = """
            insert into order_line_item_staging
                (order_id, customer_id, customer_name, product_id, product_name,
                 quantity, unit_price, order_date, processed)
            values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            on conflict (order_id, product_id) do nothing
            """;

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public void write(Chunk<? extends OrderLineItemStaging> chunk) {
        int dropped = 0;
        for (OrderLineItemStaging item : chunk) {
            int inserted = entityManager.createNativeQuery(INSERT_SQL)
                    .setParameter(1, item.getOrderId())
                    .setParameter(2, item.getCustomerId())
                    .setParameter(3, item.getCustomerName())
                    .setParameter(4, item.getProductId())
                    .setParameter(5, item.getProductName())
                    .setParameter(6, item.getQuantity())
                    .setParameter(7, item.getUnitPrice())
                    .setParameter(8, item.getOrderDate())
                    .setParameter(9, item.isProcessed())
                    .executeUpdate();
            if (inserted == 0) {
                dropped++;
                log.debug("Duplicate dropped by unique constraint, orderId={}, productId={}",
                        item.getOrderId(), item.getProductId());
            }
        }
        if (dropped > 0) {
            log.info("Dropped {} duplicate staging row(s) in chunk of {} via unique constraint",
                    dropped, chunk.size());
        }
    }
}
