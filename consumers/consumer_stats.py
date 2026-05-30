import json
import os
from collections import Counter, defaultdict

from kafka import KafkaConsumer


TOPIC_NAME = os.getenv("KAFKA_TOPIC", "ecommerce-orders")
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SUMMARY_EVERY = int(os.getenv("SUMMARY_EVERY", "50"))
TOP_N = int(os.getenv("STATS_TOP_N", "5"))


def to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def clean_text(value, default="UNKNOWN"):
    text = "" if value is None else str(value).strip()
    return text if text else default


def format_top_items(items, value_suffix=""):
    if not items:
        return "none"
    return ", ".join(f"{name}: {value:.2f}{value_suffix}" for name, value in items)


def print_summary():
    average_basket = total_amount / total_orders if total_orders else 0.0
    delivery_rate = delivered_orders / total_orders if total_orders else 0.0
    cancellation_rate = cancelled_orders / total_orders if total_orders else 0.0
    top_countries = country_revenue.most_common(TOP_N)
    top_products = product_quantity.most_common(TOP_N)
    top_customers = Counter(customer_revenue).most_common(TOP_N)

    print("\n=== REAL-TIME ECOMMERCE STATS ===", flush=True)
    print(f"Messages processed : {total_orders}", flush=True)
    print(f"Total revenue      : {total_amount:.2f}", flush=True)
    print(f"Average basket     : {average_basket:.2f}", flush=True)
    print(f"Total quantity     : {total_quantity:.2f}", flush=True)
    print(f"Delivered orders   : {delivered_orders} ({delivery_rate:.2%})", flush=True)
    print(f"Cancelled orders   : {cancelled_orders} ({cancellation_rate:.2%})", flush=True)
    print(f"Top countries      : {format_top_items(top_countries)}", flush=True)
    print(f"Top products       : {format_top_items(top_products, ' units')}", flush=True)
    print(f"Top customers      : {format_top_items(top_customers)}", flush=True)
    print("=================================\n", flush=True)


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="consumer-stats",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

total_orders = 0
total_amount = 0.0
total_quantity = 0.0
delivered_orders = 0
cancelled_orders = 0
country_revenue = Counter()
product_quantity = Counter()
customer_revenue = defaultdict(float)

print(f"Stats consumer listening on topic '{TOPIC_NAME}' via {KAFKA_SERVER}", flush=True)
print(f"Summary will be printed every {SUMMARY_EVERY} messages", flush=True)

for message in consumer:
    order = message.value
    quantity = to_float(order.get("Quantity"))
    unit_price = to_float(order.get("UnitPrice"))
    line_total = quantity * unit_price
    country = clean_text(order.get("Country"))
    stock_code = clean_text(order.get("StockCode"))
    customer_id = clean_text(order.get("CustomerID"), "GUEST")
    invoice_no = clean_text(order.get("InvoiceNo"), "")
    is_cancelled = invoice_no.startswith("C") or quantity < 0

    total_orders += 1
    total_amount += line_total
    total_quantity += quantity
    if is_cancelled:
        cancelled_orders += 1
    else:
        delivered_orders += 1
    country_revenue[country] += line_total
    product_quantity[stock_code] += quantity
    customer_revenue[customer_id] += line_total

    if total_orders % SUMMARY_EVERY == 0:
        print_summary()
