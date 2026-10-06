package com.batch.demo.repository;

import java.util.Collection;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.batch.demo.domain.Order;

public interface OrderRepository extends JpaRepository<Order, Long> {

    /** The subset of {@code orderNumbers} that already has an Order - one query for a whole page. */
    @Query("select o.orderNumber from Order o where o.orderNumber in :orderNumbers")
    List<String> findExistingOrderNumbers(@Param("orderNumbers") Collection<String> orderNumbers);
}
