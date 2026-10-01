package com.batch.demo.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

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
}
