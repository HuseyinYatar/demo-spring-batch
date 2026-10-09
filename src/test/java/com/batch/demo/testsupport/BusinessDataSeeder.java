package com.batch.demo.testsupport;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Puts one row into every business table, linked the way the application links them
 * (line item and invoice reference the order), so a test can tell whether something
 * emptied all of them. Plain SQL, so it works before any job has run - and, for
 * TruncateBusinessTablesOnStartupTest, while the application context is still starting.
 */
public final class BusinessDataSeeder {

    private BusinessDataSeeder() {
    }

    public static void seed(JdbcTemplate jdbc) {
        Long orderId = jdbc.queryForObject("""
                insert into orders (id, order_number, customer_id, customer_name, order_date, status, created_at)
                values (nextval('orders_seq'), 'ORD-SEED-1', 'C-1', 'Seed Customer', current_date, 'INVOICED', now())
                returning id
                """, Long.class);
        jdbc.update("""
                insert into order_line_item (id, order_id, product_id, product_name, quantity, unit_price, line_total)
                values (nextval('order_line_item_seq'), ?, 'P-1', 'Seed Product', 2, 10.00, 20.00)
                """, orderId);
        jdbc.update("""
                insert into invoice (id, order_id, invoice_number, subtotal, tax_amount, total_amount, issued_date)
                values (nextval('invoice_seq'), ?, 'INV-SEED-1', 20.00, 3.60, 23.60, current_date)
                """, orderId);
        insertStagingRow(jdbc);
        jdbc.update("""
                insert into daily_sales_report (business_date, invoice_count, total_subtotal, total_tax, total_amount,
                                                top_customer_name, top_customer_total, generated_at)
                values (current_date, 1, 20.00, 3.60, 23.60, 'Seed Customer', 23.60, now())
                """);
    }

    /** The staging id is an identity column, so the returned id shows where its sequence stands. */
    public static long insertStagingRow(JdbcTemplate jdbc) {
        Long id = jdbc.queryForObject("""
                insert into order_line_item_staging (order_id, customer_id, customer_name, product_id, product_name,
                                                     quantity, unit_price, order_date, processed)
                values ('ORD-SEED-1', 'C-1', 'Seed Customer', 'P-1', 'Seed Product', 2, 10.00, current_date, false)
                returning id
                """, Long.class);
        return id == null ? -1 : id;
    }
}
