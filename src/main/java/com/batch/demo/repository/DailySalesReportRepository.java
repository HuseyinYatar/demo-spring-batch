package com.batch.demo.repository;

import java.time.LocalDate;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.batch.demo.domain.DailySalesReport;

public interface DailySalesReportRepository extends JpaRepository<DailySalesReport, Long> {

    Optional<DailySalesReport> findByBusinessDate(LocalDate businessDate);
}
