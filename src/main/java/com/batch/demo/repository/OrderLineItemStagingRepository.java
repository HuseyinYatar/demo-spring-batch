package com.batch.demo.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.batch.demo.domain.OrderLineItemStaging;

public interface OrderLineItemStagingRepository extends JpaRepository<OrderLineItemStaging, Long> {

    @Query("select distinct s.orderId from OrderLineItemStaging s where s.processed = false order by s.orderId")
    List<String> findDistinctUnprocessedOrderIds();

    List<OrderLineItemStaging> findByOrderIdAndProcessedFalse(String orderId);
}
