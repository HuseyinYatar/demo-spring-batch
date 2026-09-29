package com.batch.demo.repository;

import java.time.LocalDate;
import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.batch.demo.domain.Invoice;

public interface InvoiceRepository extends JpaRepository<Invoice, Long> {

    @Query("select count(i) as invoiceCount, coalesce(sum(i.subtotal),0) as totalSubtotal, "
            + "coalesce(sum(i.taxAmount),0) as totalTax, coalesce(sum(i.totalAmount),0) as totalAmount "
            + "from Invoice i where i.issuedDate = :issuedDate")
    DailySalesAggregate aggregateByIssuedDate(@Param("issuedDate") LocalDate issuedDate);

    @Query("select i.order.customerName as customerName, sum(i.totalAmount) as totalSpend "
            + "from Invoice i where i.issuedDate = :issuedDate "
            + "group by i.order.customerName order by sum(i.totalAmount) desc")
    List<CustomerSpend> findTopCustomersByIssuedDate(@Param("issuedDate") LocalDate issuedDate, Pageable pageable);
}
