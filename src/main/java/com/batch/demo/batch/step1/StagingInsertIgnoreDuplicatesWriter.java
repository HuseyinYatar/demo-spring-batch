package com.batch.demo.batch.step1;

import java.sql.PreparedStatement;
import java.util.List;

import org.hibernate.Session;
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
 * Uses the transaction-bound {@link EntityManager}'s Hibernate session (not JdbcTemplate) so a
 * real connection failure still surfaces as Hibernate's own JDBCConnectionException, which
 * ingestLineItemsWorkerStep's retry policy is registered for. Hibernate's own JDBC batching
 * doesn't apply to native SQL, so the chunk is sent as one explicit JDBC batch instead of a
 * round trip per row.
 */
public class StagingInsertIgnoreDuplicatesWriter implements ItemWriter<OrderLineItemStaging> {

    private static final Logger log = LoggerFactory.getLogger(StagingInsertIgnoreDuplicatesWriter.class);

    private static final String INSERT_SQL = """
            insert into order_line_item_staging
                (order_id, customer_id, customer_name, product_id, product_name,
                 quantity, unit_price, order_date, processed)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict (order_id, product_id) do nothing
            """;

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public void write(Chunk<? extends OrderLineItemStaging> chunk) {
        List<? extends OrderLineItemStaging> items = chunk.getItems();
        // One JDBC batch per chunk on the transaction's own connection. Hibernate's
        // doWork converts a SQLException (including a connection failure) into its own
        // JDBCException hierarchy, so the step's retry registration still matches.
        int[] updateCounts = entityManager.unwrap(Session.class).doReturningWork(connection -> {
            try (PreparedStatement ps = connection.prepareStatement(INSERT_SQL)) {
                for (OrderLineItemStaging item : items) {
                    ps.setObject(1, item.getOrderId());
                    ps.setObject(2, item.getCustomerId());
                    ps.setObject(3, item.getCustomerName());
                    ps.setObject(4, item.getProductId());
                    ps.setObject(5, item.getProductName());
                    ps.setObject(6, item.getQuantity());
                    ps.setObject(7, item.getUnitPrice());
                    ps.setObject(8, item.getOrderDate());
                    ps.setObject(9, item.isProcessed());
                    ps.addBatch();
                }
                return ps.executeBatch();
            }
        });

        // Without reWriteBatchedInserts the driver reports an exact count per row (0 =
        // conflict, row dropped); with it every entry is SUCCESS_NO_INFO and nothing is
        // counted here.
        int dropped = 0;
        for (int i = 0; i < updateCounts.length; i++) {
            if (updateCounts[i] == 0) {
                dropped++;
                log.debug("Duplicate dropped by unique constraint, orderId={}, productId={}",
                        items.get(i).getOrderId(), items.get(i).getProductId());
            }
        }
        if (dropped > 0) {
            log.info("Dropped {} duplicate staging row(s) in chunk of {} via unique constraint",
                    dropped, chunk.size());
        }
    }
}
