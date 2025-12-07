import csv
import random
import argparse
from datetime import datetime, timedelta
from collections import defaultdict

# ---------- Configuration ----------

CHAINS_CSV = "chains.csv"
MERCHANTS_CSV = "merchants.csv"
PRODUCTS_CSV = "products.csv"
OUTPUT_CSV = "transactions.csv"

# Default values (can be overridden by command-line arguments)
WINDOW_MINUTES = 30
ORDERS_PER_SEC_MIN = 20
ORDERS_PER_SEC_MAX = 30

FAILED_PROB = 0.03  # ~3% failed
RANDOM_SEED = 42    # for reproducibility; change/remove for more randomness

random.seed(RANDOM_SEED)

# ---------- Command-line argument parsing ----------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate transaction data for timeseries testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 30 minutes of data with 20-30 orders per second
  python3 generate_transactions.py

  # Generate 60 minutes of data with 10-20 orders per second
  python3 generate_transactions.py --window-minutes 60 --orders-per-sec-min 10 --orders-per-sec-max 20

  # Generate 10 minutes of data with 5-15 orders per second (for testing multiple time windows)
  python3 generate_transactions.py --window-minutes 10 --orders-per-sec-min 5 --orders-per-sec-max 15
        """
    )
    parser.add_argument(
        "--window-minutes",
        type=int,
        default=WINDOW_MINUTES,
        help=f"Time window in minutes (default: {WINDOW_MINUTES})"
    )
    parser.add_argument(
        "--orders-per-sec-min",
        type=int,
        default=ORDERS_PER_SEC_MIN,
        help=f"Minimum orders per second (default: {ORDERS_PER_SEC_MIN})"
    )
    parser.add_argument(
        "--orders-per-sec-max",
        type=int,
        default=ORDERS_PER_SEC_MAX,
        help=f"Maximum orders per second (default: {ORDERS_PER_SEC_MAX})"
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_CSV,
        help=f"Output CSV file (default: {OUTPUT_CSV})"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=RANDOM_SEED,
        help=f"Random seed for reproducibility (default: {RANDOM_SEED})"
    )

    return parser.parse_args()

# ---------- Helpers to load config ----------

def load_chains(path=CHAINS_CSV):
    chains = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["traffic_weight"] = float(row.get("traffic_weight", 1.0))
            chains.append(row)
    return chains

def load_merchants(path=MERCHANTS_CSV):
    merchants = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            merchants.append(row)
    return merchants

def load_products(path=PRODUCTS_CSV):
    products = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["base_price"] = float(row["base_price"])
            products.append(row)
    return products

# ---------- Sampling utilities ----------

def choose_quantity():
    r = random.random()
    if r < 0.80:
        return 1
    elif r < 0.95:
        return 2
    elif r < 0.99:
        return 3
    else:
        return 4

def choose_status():
    return "FAILED" if random.random() < FAILED_PROB else "COMPLETE"

# ---------- Main generation logic ----------

def build_chain_index(chains):
    """Map chain_id -> chain metadata and prepare weights."""
    chain_by_id = {c["chain_id"]: c for c in chains}
    return chain_by_id

def build_merchant_pool(merchants, chain_by_id):
    """Return list of merchants with attached chain info."""
    enriched = []
    for m in merchants:
        chain_id = m["chain_id"]
        chain = chain_by_id.get(chain_id)
        if not chain:
            continue
        enriched.append({
            "merchant_id": m["merchant_id"],
            "chain_id": chain_id,
            "chain_name": chain["chain_name"],
            "traffic_weight": chain["traffic_weight"],
            "city": m.get("city", "")
        })
    return enriched

def build_series_combinations(merchant_pool, products):
    """
    Precompute all (chain, merchant, product) combos.
    This set defines your potential unique series.
    """
    series = []
    for m in merchant_pool:
        for p in products:
            series.append({
                "chain": m["chain_name"],
                "merchant_id": m["merchant_id"],
                "city": m["city"],
                "product_type": p["product_type"],
                "product_item": p["product_item"],
                "base_price": p["base_price"]
            })
    return series

def price_for_item(base_price, quantity):
    # add a small +/- variation to mimic tax/discounts etc.
    factor = 1.0 + random.uniform(-0.05, 0.10)
    return round(base_price * quantity * factor, 2)

def generate_transactions(window_minutes=WINDOW_MINUTES,
                         orders_per_sec_min=ORDERS_PER_SEC_MIN,
                         orders_per_sec_max=ORDERS_PER_SEC_MAX,
                         output_csv=OUTPUT_CSV,
                         random_seed=RANDOM_SEED):
    """Generate transaction data.

    Args:
        window_minutes: Time window in minutes
        orders_per_sec_min: Minimum orders per second
        orders_per_sec_max: Maximum orders per second
        output_csv: Output CSV file path
        random_seed: Random seed for reproducibility
    """
    # Set random seed
    random.seed(random_seed)

    chains = load_chains()
    chain_by_id = build_chain_index(chains)
    merchants = load_merchants()
    products = load_products()

    merchant_pool = build_merchant_pool(merchants, chain_by_id)
    if not merchant_pool:
        raise RuntimeError("No valid merchants loaded. Check merchants.csv and chains.csv.")

    series_combos = build_series_combinations(merchant_pool, products)
    if not series_combos:
        raise RuntimeError("No series combinations found. Check merchants.csv and products.csv.")

    print(f"Loaded {len(chains)} chains, {len(merchant_pool)} merchants, "
          f"{len(products)} products -> {len(series_combos)} potential (merchant, product) series.")
    print(f"Generating {window_minutes} minutes of data with {orders_per_sec_min}-{orders_per_sec_max} orders/sec...")

    # Per-merchant order counter to ensure order_num uniqueness within merchant_id
    merchant_order_counters = defaultdict(int)
    # Per-merchant running time to emulate realistic order sequences
    merchant_last_time = defaultdict(lambda: datetime.now().replace(microsecond=0) - timedelta(minutes=window_minutes+5))

    start_time = datetime.now().replace(microsecond=0) - timedelta(minutes=window_minutes+5)
    end_time = start_time + timedelta(minutes=window_minutes)

    with open(output_csv, "w", newline="") as f:
        fieldnames = [
            "chain",
            "merchant_id",
            "city",
            "product_type",
            "product_item",
            "order_num",
            "quantity",
            "transaction_time",
            "order_status",
            "price"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        total_orders = 0
        unique_series_seen = set()

        # Generate orders across the time window
        current_time = start_time
        while current_time < end_time:
            # How many orders in this second
            per_sec = random.randint(orders_per_sec_min, orders_per_sec_max)

            for _ in range(per_sec):
                combo = random.choice(series_combos)

                merchant_id = combo["merchant_id"]
                merchant_order_counters[merchant_id] += 1
                order_num = merchant_order_counters[merchant_id]

                quantity = choose_quantity()
                status = choose_status()
                price = price_for_item(combo["base_price"], quantity)

                # Increment merchant's running time with jitter to emulate realistic order sequence
                # Add a small random delay (0-100ms) from the previous order for this merchant

                # Ensure that we are not behind the current time
                merchant_last_time[merchant_id] = max(merchant_last_time[merchant_id], current_time)

                jitter_us = random.randint(0, 100_000)
                merchant_last_time[merchant_id] += timedelta(microseconds=jitter_us)

                # Ensure that we are not breaking into the next second
                merchant_last_time[merchant_id] = min(merchant_last_time[merchant_id], current_time + timedelta(seconds=1))

                txn_time = merchant_last_time[merchant_id]

                row = {
                    "chain": combo["chain"],
                    "merchant_id": merchant_id,
                    "city": combo["city"],
                    "product_type": combo["product_type"],
                    "product_item": combo["product_item"],
                    "order_num": order_num,
                    "quantity": quantity,
                    "transaction_time": txn_time.isoformat(),
                    "order_status": status,
                    "price": price,
                }

                writer.writerow(row)
                total_orders += 1
                unique_series_seen.add(
                    (combo["chain"], merchant_id, combo["product_type"], combo["product_item"])
                )

            current_time += timedelta(seconds=1)

    print(f"Done. Wrote {total_orders} orders to {output_csv}.")
    print(f"Unique (chain, merchant_id, product_type, product_item) (excluding order_num) series: {len(unique_series_seen)}.")
    print(f"Time range: {start_time.isoformat()} to {end_time.isoformat()}")


if __name__ == "__main__":
    args = parse_args()
    generate_transactions(
        window_minutes=args.window_minutes,
        orders_per_sec_min=args.orders_per_sec_min,
        orders_per_sec_max=args.orders_per_sec_max,
        output_csv=args.output,
        random_seed=args.seed
    )

