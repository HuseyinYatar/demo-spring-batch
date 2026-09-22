import csv
import random
from datetime import datetime, timedelta

products = [
    ("P101", "Kablosuz Mouse", 25.50),
    ("P102", "Mekanik Klavye", 75.00),
    ("P103", "27 inç Monitör", 220.00),
    ("P104", "USB-C Hub", 15.30),
    ("P105", "Kulaklık Standı", 12.00)
]

TOTAL_ROWS = 100_000
# Kept well under batch.skip-limit (20 by default) so the job still completes
# successfully while still exercising the skip-and-log path.
BAD_ROW_COUNT = 14
OUTPUT_PATH = "src/main/resources/data/order-line-items.csv"

rows = []
start_date = datetime(2025, 1, 1)
minute_offset = 0
order_index = 0

# Most orders get one line item, some get two or three, so buildInvoicesStep's
# per-order aggregation actually has multiple line items to combine sometimes.
while len(rows) < TOTAL_ROWS:
    order_index += 1
    order_id = f"ORD-{order_index:06d}"
    customer_id = f"CUST-{random.randint(100, 999)}"
    customer_name = f"Müşteri {random.randint(1, 500)}"
    line_item_count = random.choices([1, 2, 3], weights=[70, 20, 10])[0]

    for _ in range(line_item_count):
        if len(rows) >= TOTAL_ROWS:
            break
        minute_offset += 1
        prod = random.choice(products)
        rows.append([
            order_id,
            customer_id,
            customer_name,
            prod[0],
            prod[1],
            random.randint(1, 5),
            prod[2],
            # Date-only, matching OrderLineFieldSetMapper's LocalDate.parse(...) -
            # the previous "%Y-%m-%d %H:%M:%S" format failed to parse and broke
            # every run.
            (start_date + timedelta(minutes=minute_offset)).strftime("%Y-%m-%d")
        ])

# Deliberately-invalid rows so ingestLineItemsStep's skip-and-log path (see
# CLAUDE.md) still has something real to demonstrate: negative qty/price and
# blank customer/order id are business-rule violations caught by
# DefaultOrderLineValidator (skip on process); non-numeric price, a malformed
# date, and a wrong column count fail to parse at all (skip on read).
bad_row_variants = [
    lambda r: r[:5] + [-3] + r[6:8],
    lambda r: r[:6] + [-10.0] + r[7:8],
    lambda r: r[:2] + [""] + r[3:],
    lambda r: [""] + r[1:],
    lambda r: r[:6] + ["not-a-number"] + r[7:8],
    lambda r: r[:7] + ["2025-13-40"],
    lambda r: r[:7],
]

bad_row_positions = random.sample(range(len(rows)), BAD_ROW_COUNT)
for i, pos in enumerate(bad_row_positions):
    variant = bad_row_variants[i % len(bad_row_variants)]
    rows[pos] = variant(rows[pos])

with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["orderId", "customerId", "customerName", "productId",
                      "productName", "quantity", "unitPrice", "orderDate"])
    writer.writerows(rows)
