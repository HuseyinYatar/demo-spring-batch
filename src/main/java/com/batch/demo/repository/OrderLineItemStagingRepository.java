package com.batch.demo.repository;

import java.util.Collection;
import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import com.batch.demo.domain.OrderLineItemStaging;

public interface OrderLineItemStagingRepository extends JpaRepository<OrderLineItemStaging, Long> {

    /**
     * Splits the sorted distinct unprocessed order ids into at most gridSize
     * contiguous, near-equal buckets (ntile) and returns only each bucket's
     * [min, max] - so the database does the sorting/slicing and at most gridSize
     * rows ever reach the JVM. Each row is {fromOrderId, toOrderId}, ordered by
     * bucket. Fewer rows than gridSize come back when there are fewer distinct
     * orders than partitions.
     */
    @Query(value = "select min(order_id), max(order_id) from ("
            + "  select order_id, ntile(:gridSize) over (order by order_id) as bucket from ("
            + "    select distinct order_id from order_line_item_staging where processed = false"
            + "  ) d"
            + ") t group by bucket order by bucket", nativeQuery = true)
    List<Object[]> findUnprocessedOrderIdRanges(@Param("gridSize") int gridSize);

    /**
     * Keyset-paginated: returns at most one page's worth of distinct unprocessed
     * order ids in [fromOrderId, toOrderId], strictly after afterOrderId (null on
     * the first page). Callers page through by re-invoking with the last id seen,
     * so no single call materializes more than pageable's page size.
     */
    @Query("select distinct s.orderId from OrderLineItemStaging s where s.processed = false "
            + "and s.orderId between :fromOrderId and :toOrderId "
            + "and (:afterOrderId is null or s.orderId > :afterOrderId) "
            + "order by s.orderId")
    List<String> findDistinctUnprocessedOrderIdsBetweenAfter(
            @Param("fromOrderId") String fromOrderId,
            @Param("toOrderId") String toOrderId,
            @Param("afterOrderId") String afterOrderId,
            Pageable pageable);

    List<OrderLineItemStaging> findByOrderIdAndProcessedFalse(String orderId);

    /**
     * Every unprocessed line of the given orders in one query, in staging order (id) so an
     * order's first line is its earliest-staged one, as with the per-order lookup.
     */
    @Query("select s from OrderLineItemStaging s where s.orderId in :orderIds and s.processed = false "
            + "order by s.id")
    List<OrderLineItemStaging> findUnprocessedByOrderIdIn(@Param("orderIds") Collection<String> orderIds);

    /**
     * Flips the given staging rows to processed with one statement, instead of loading
     * them as managed entities and letting dirty checking issue an UPDATE per row. Runs
     * when called (inside the writer), not at commit, so it stays inside the step's retry
     * scope and the repository's exception translation. Joins the chunk's transaction; the
     * annotation is there because declared query methods get no transaction of their own.
     */
    @Transactional
    @Modifying
    @Query("update OrderLineItemStaging s set s.processed = true where s.id in :ids")
    int markProcessed(@Param("ids") Collection<Long> ids);
}
