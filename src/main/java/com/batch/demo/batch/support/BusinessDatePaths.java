package com.batch.demo.batch.support;

import java.time.LocalDate;

/**
 * Inserts a run's businessDate before the extension of a configured output path
 * (invoice-summary.csv -> invoice-summary-2026-10-01.csv), so each business date
 * gets its own file instead of every run overwriting the previous one. Shared by
 * every writer of such a file so they can never disagree on the naming scheme.
 */
public final class BusinessDatePaths {

    private BusinessDatePaths() {
    }

    public static String withBusinessDate(String basePath, LocalDate businessDate) {
        // Only look for the extension in the file name, not in a directory like "out.d/summary".
        int nameStart = Math.max(basePath.lastIndexOf('/'), basePath.lastIndexOf('\\')) + 1;
        int dot = basePath.lastIndexOf('.');
        String suffix = "-" + businessDate;
        return dot < nameStart
                ? basePath + suffix
                : basePath.substring(0, dot) + suffix + basePath.substring(dot);
    }
}
