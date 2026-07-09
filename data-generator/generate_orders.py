"""
E-Commerce Order Generator
---------------------------
Produces synthetic-but-realistic order events and pushes them into a
Kinesis Data Stream. This simulates the "producer" side of a real
streaming pipeline (think: a checkout service emitting an event every
time a customer places an order).

Usage:
    python generate_orders.py --stream ecommerce-orders-stream --rate 5
    python generate_orders.py --stream ecommerce-orders-stream --total 500

Flags:
    --stream   Kinesis stream name
    --region   AWS region (default: us-east-1)
    --rate     Orders per second (default: 3)
    --total    Stop after N orders (default: run forever)
    --seed     Random seed for reproducible runs (optional)
"""                                                                     

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

import boto3

# Reference data used to make orders look realistic.

PRODUCTS = [
    # (product_id, name, category, base_price)
    ("P1001", "Wireless Noise-Cancelling Headphones", "Electronics", 199.99),
    ("P1002", "Mechanical Keyboard",                  "Electronics", 89.99),
    ("P1003", "4K Webcam",                            "Electronics", 129.99),
    ("P2001", "Running Shoes",                        "Footwear",    119.99),
    ("P2002", "Hiking Boots",                         "Footwear",    159.99),
    ("P3001", "Stainless Steel Water Bottle",         "Home",        24.99),
    ("P3002", "Ceramic Coffee Mug Set",               "Home",        34.99),
    ("P3003", "Air Fryer",                            "Home",        99.99),
    ("P4001", "Yoga Mat",                             "Fitness",     39.99),
    ("P4002", "Adjustable Dumbbells",                 "Fitness",     299.99),
    ("P5001", "Bestselling Novel",                    "Books",       14.99),
    ("P5002", "Data Engineering Handbook",            "Books",       49.99),
]

PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "GIFT_CARD", "APPLE_PAY"]
ORDER_STATUSES = ["PLACED", "PLACED", "PLACED", "PLACED", "CANCELLED"]  # ~20% cancelled
CITIES = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"),
    ("Houston", "TX"), ("Phoenix", "AZ"), ("Seattle", "WA"),
    ("Boston", "MA"), ("Miami", "FL"), ("Denver", "CO"), ("Atlanta", "GA"),
]


def build_order():
    """Create a single realistic order event as a dict."""
    product_id, name, category, base_price = random.choice(PRODUCTS)
    quantity = random.randint(1, 4)

    # Add a little price noise so totals are not always identical.
    unit_price = round(base_price * random.uniform(0.95, 1.05), 2)
    total_amount = round(unit_price * quantity, 2)
    city, state = random.choice(CITIES)

    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"C{random.randint(1000, 9999)}",
        "product_id": product_id,
        "product_name": name,
        "category": category,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "currency": "USD",
        "payment_method": random.choice(PAYMENT_METHODS),
        "order_status": random.choice(ORDER_STATUSES),
        "shipping_city": city,
        "shipping_state": state,
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    parser = argparse.ArgumentParser(description="Stream fake orders to Kinesis.")
    parser.add_argument("--stream", required=True, help="Kinesis stream name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--rate", type=float, default=3.0, help="Orders per second")
    parser.add_argument("--total", type=int, default=0, help="Stop after N orders (0 = infinite)")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    client = boto3.client("kinesis", region_name=args.region)
    interval = 1.0 / args.rate if args.rate > 0 else 0
    sent = 0

    print(f"Streaming orders to '{args.stream}' in {args.region} "
          f"at ~{args.rate}/s. Press Ctrl+C to stop.")

   
   
   
    try:
        while True:
            order = build_order()
            # PartitionKey controls which shard a record lands on.
            # Using customer_id spreads load and keeps a customer's
            # events ordered relative to each other.
            client.put_record(
                StreamName=args.stream,
                Data=json.dumps(order).encode("utf-8"),
                PartitionKey=order["customer_id"],
            )
            sent += 1

            if order["total_amount"] > 250:
                flag = "  <-- HIGH VALUE"
            else:
                flag = ""
            print(f"[{sent:>5}] {order['order_id'][:8]}  "
                  f"{order['category']:<12} ${order['total_amount']:>8.2f}{flag}")

            if args.total and sent >= args.total:
                print(f"\nDone. Sent {sent} orders.")
                break

            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\nStopped. Sent {sent} orders.")


if __name__ == "__main__":
    main()
