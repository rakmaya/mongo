import csv
import random
from datetime import datetime, timedelta
from collections import defaultdict

# ---------- Configuration ----------

CHAINS_CSV = "chains.csv"
MERCHANTS_CSV = "merchants.csv"
PRODUCTS_CSV = "products.csv"
OUTPUT_CSV = "transactions.csv"

WINDOW_MINUTES = 30

# Orders per second: tweak this band to hit ~50k over 30 minutes
ORDERS_PER_SEC_MIN = 20
ORDERS_PER_SEC_MAX = 30

FAILED_PROB = 0.03  # ~3% failed
RANDOM_SEED = 42    # for reproducibility; change/remove for more randomness

random.seed(RANDOM_SEED)

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
                "product_type": p["product_type"],
                "product_item": p["product_item"],
                "base_price": p["base_price"]
            })
    return series

def price_for_item(base_price, quantity):
    # add a small +/- variation to mimic tax/discounts etc.
    factor = 1.0 + random.uniform(-0.05, 0.10)
    return round(base_price * quantity * factor, 2)

def generate_transactions():
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

    # Per-merchant order counter to ensure order_num uniqueness within merchant_id
    merchant_order_counters = defaultdict(int)

    start_time = datetime.now().replace(microsecond=0) - timedelta(minutes=35)
    end_time = start_time + timedelta(minutes=WINDOW_MINUTES)

    current_time = start_time

    with open(OUTPUT_CSV, "w", newline="") as f:
        fieldnames = [
            "chain",
            "merchant_id",
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

        while current_time < end_time:
            # How many orders in this second
            per_sec = random.randint(ORDERS_PER_SEC_MIN, ORDERS_PER_SEC_MAX)

            for _ in range(per_sec):
                combo = random.choice(series_combos)

                merchant_id = combo["merchant_id"]
                merchant_order_counters[merchant_id] += 1
                order_num = merchant_order_counters[merchant_id]

                quantity = choose_quantity()
                status = choose_status()
                price = price_for_item(combo["base_price"], quantity)

                # add sub-second jitter to spread within the second
                jitter_us = random.randint(0, 999_999)
                txn_time = current_time + timedelta(microseconds=jitter_us)

                row = {
                    "chain": combo["chain"],
                    "merchant_id": merchant_id,
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

    print(f"Done. Wrote {total_orders} orders to {OUTPUT_CSV}.")
    print(f"Unique (chain, merchant_id, product_type, product_item) series: {len(unique_series_seen)}.")


if __name__ == "__main__":
    generate_transactions()

