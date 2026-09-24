package com.batch.demo.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.batch.demo.domain.OrderLineItemStaging;

public interface OrderLineItemStagingRepository extends JpaRepository<OrderLineItemStaging, Long> {

    @Query("select distinct s.orderId from OrderLineItemStaging s where s.processed = false order by s.orderId")
    List<String> findDistinctUnprocessedOrderIds();

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

    boolean existsByOrderIdAndProductId(String orderId, String productId);
}
