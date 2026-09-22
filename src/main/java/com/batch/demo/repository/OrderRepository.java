package com.batch.demo.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.batch.demo.domain.Order;

public interface OrderRepository extends JpaRepository<Order, Long> {

    boolean existsByOrderNumber(String orderNumber);
}
