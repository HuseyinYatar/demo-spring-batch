package com.batch.demo;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;

import com.batch.demo.batch.support.BusinessDatePaths;

import static org.assertj.core.api.Assertions.assertThat;

class BusinessDatePathsTest {

    private static final LocalDate DATE = LocalDate.of(2026, 10, 1);

    @Test
    void insertsTheDateBeforeTheExtension() {
        assertThat(BusinessDatePaths.withBusinessDate("invoice-summary.csv", DATE))
                .isEqualTo("invoice-summary-2026-10-01.csv");
    }

    @Test
    void keepsTheDirectoryAndOnlyTouchesTheFileName() {
        assertThat(BusinessDatePaths.withBusinessDate("out.d/rejected-rows.csv", DATE))
                .isEqualTo("out.d/rejected-rows-2026-10-01.csv");
    }

    @Test
    void appendsTheDateWhenThereIsNoExtension() {
        assertThat(BusinessDatePaths.withBusinessDate("out.d/summary", DATE))
                .isEqualTo("out.d/summary-2026-10-01");
    }
}
