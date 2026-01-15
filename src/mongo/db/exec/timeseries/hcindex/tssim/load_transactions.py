#!/usr/bin/env python3
"""
Load transactions.csv into MongoDB as a timeseries collection.

Usage:
    python3 load_transactions.py [--hcindex] [--db DATABASE] [--collection COLLECTION]

Options:
    --hcindex       Create collection with HCIndex enabled (default: disabled)
                    When enabled, uses 5-minute time windows (period: minute, frequency: 5)
    --db DATABASE   Database name (default: metrics)
    --collection    Collection name (default: transactions_hc or transactions_no_hc)
    --uri URI       MongoDB connection URI (default: mongodb://127.0.0.1:27017)
    --csv PATH      Path to transactions.csv (default: transactions.csv)
    --rows N        Number of rows to load (default: 10, 0 = all rows)
    --insert-mode   Insert mode: 'batch' (insert_many) or 'single' (insert_one) (default: batch)
                    Use 'single' to simulate real-time ingestion one document at a time
"""

import csv
import sys
import argparse
import time
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import OperationFailure

def parse_args():
    parser = argparse.ArgumentParser(description="Load transactions into MongoDB timeseries collection")
    parser.add_argument("--hcindex", action="store_true", help="Enable HCIndex for the collection")
    parser.add_argument("--db", default="metrics", help="Database name (default: metrics)")
    parser.add_argument("--collection", default=None, help="Collection name (default: transactions_hc or transactions_no_hc)")
    parser.add_argument("--uri", default="mongodb://127.0.0.1:27017", help="MongoDB connection URI")
    parser.add_argument("--csv", default="transactions.csv", help="Path to transactions.csv")
    parser.add_argument("--rows", type=int, default=10, help="Number of rows to load (default: 100, 0 = all rows)")
    parser.add_argument("--insert-mode", choices=["batch", "single"], default="batch",
                        help="Insert mode: 'batch' (insert_many) or 'single' (insert_one) (default: batch)")
    args = parser.parse_args()

    # Set default collection name based on hcindex flag
    if args.collection is None:
        args.collection = "transactions_hc" if args.hcindex else "transactions_no_hc"

    return args

def create_timeseries_collection(db, collection_name, use_hcindex=False):
    """Create a timeseries collection with optional HCIndex."""
    try:
        db.drop_collection(collection_name)
        print(f"Dropped existing collection: {collection_name}")
    except Exception as e:
        print(f"Note: {e}")

    # Build create collection command
    timeseries_config = {
        "timeField": "transaction_time",
        "metaField": "metadata"
    }

    if use_hcindex:
        timeseries_config["useHCIndex"] = True
        timeseries_config["hcindexOptions"] = {
            "period": "minute",
            "frequency": 1,
            "excludedColumns": ["product_item", "order_num", "order_status"]
        }

    create_cmd = {
        "timeseries": timeseries_config
    }

    try:
        db.create_collection(collection_name, **create_cmd)
        mode = "with HCIndex" if use_hcindex else "without HCIndex"
        print(f"✓ Created timeseries collection '{collection_name}' {mode}")
        return True
    except OperationFailure as e:
        print(f"✗ Failed to create collection: {e}")
        return False

def load_transactions(csv_path, db, collection_name, max_rows=0, insert_mode="batch"):
    """Load transactions from CSV into MongoDB.

    Args:
        csv_path: Path to transactions.csv
        db: MongoDB database
        collection_name: Collection name
        max_rows: Maximum rows to load (0 = all rows)
        insert_mode: 'batch' for insert_many, 'single' for insert_one

    Returns:
        Tuple of (success: bool, cardinality_stats: dict)
    """
    collection = db[collection_name]

    documents = []
    unique_combinations = set()

    # Track individual field cardinalities
    unique_chains = set()
    unique_merchant_ids = set()
    unique_product_types = set()
    unique_product_items = set()
    unique_order_nums = set()
    unique_order_statuses = set()
    unique_cities = set()

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            # Stop if we've reached max_rows (and max_rows > 0)
            if max_rows > 0 and i >= max_rows:
                break

            # Parse transaction_time to datetime
            txn_time = datetime.fromisoformat(row['transaction_time'])

            # Build document with metadata
            doc = {
                "transaction_time": txn_time,
                "metadata": {
                    "chain": row['chain'],
                    "merchant_id": row['merchant_id'],
                    "product_type": row['product_type'],
                    "product_item": row['product_item'],
                    "order_num": row['order_num'],
                    "order_status": row['order_status'],
                    "city": row.get('city', '')
                },
                "quantity": int(row['quantity']),
                "price": float(row['price'])
            }
            documents.append(doc)

            # Track unique combinations
            unique_combo = (
                row['chain'],
                row['merchant_id'],
                row['product_type'],
                row['product_item'],
                row['order_num'],
                row['order_status'],
                row.get('city', '')
            )
            unique_combinations.add(unique_combo)

            # Track individual field cardinalities
            unique_chains.add(row['chain'])
            unique_merchant_ids.add(row['merchant_id'])
            unique_product_types.add(row['product_type'])
            unique_product_items.add(row['product_item'])
            unique_order_nums.add(row['order_num'])
            unique_order_statuses.add(row['order_status'])
            unique_cities.add(row.get('city', ''))

    print(f"Loaded {len(documents)} documents from {csv_path}")

    # Insert based on mode
    start_perf_time = time.perf_counter()

    if insert_mode == "single":
        # Insert one document at a time (simulates real-time ingestion)
        print(f"Using insert_one mode (single document insertion)...")
        inserted_count = 0
        for i, doc in enumerate(documents):
            try:
                result = collection.insert_one(doc)
                inserted_count += 1
                # Print progress every 100 documents
                if (i + 1) % 1000 == 0:
                    print(f"  Inserted {i + 1} documents...")
            except Exception as e:
                print(f"✗ Error inserting document {i}: {e}")
                return False, {}
        print(f"✓ Successfully inserted {inserted_count} transactions using insert_one")
    else:
        # Insert in batches using insert_many (default)
        print(f"Using insert_many mode (batch insertion)...")
        batch_size = 1000
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i+batch_size]
            try:
                result = collection.insert_many(batch)
                print(f"  Inserted batch {i//batch_size + 1}: {len(result.inserted_ids)} documents")
            except Exception as e:
                print(f"✗ Error inserting batch: {e}")
                return False, {}
        print(f"✓ Successfully inserted {len(documents)} transactions using insert_many")

    end_perf_time = time.perf_counter()
    execution_time = (end_perf_time - start_perf_time) * 1000
    print(f"Total insertion time: {execution_time:.3f}ms ({execution_time/len(documents):.3f}ms per document)")

    # Prepare cardinality statistics
    cardinality_stats = {
        "total_documents": len(documents),
        "unique_combinations": len(unique_combinations),
        "unique_chains": len(unique_chains),
        "unique_merchant_ids": len(unique_merchant_ids),
        "unique_product_types": len(unique_product_types),
        "unique_product_items": len(unique_product_items),
        "unique_order_nums": len(unique_order_nums),
        "unique_order_statuses": len(unique_order_statuses),
        "unique_cities": len(unique_cities)
    }

    return True, cardinality_stats

def main():
    args = parse_args()

    # Connect to MongoDB
    try:
        client = MongoClient(args.uri)
        client.admin.command('ping')
        print(f"✓ Connected to MongoDB at {args.uri}")
    except Exception as e:
        print(f"✗ Failed to connect to MongoDB: {e}")
        sys.exit(1)

    db = client[args.db]

    # Create collection
    if not create_timeseries_collection(db, args.collection, args.hcindex):
        sys.exit(1)

    # Load transactions
    success, cardinality_stats = load_transactions(args.csv, db, args.collection, args.rows, args.insert_mode)
    if not success:
        sys.exit(1)

    # Print summary
    collection = db[args.collection]
    count = collection.count_documents({})
    print(f"\n✓ Collection '{args.collection}' now contains {count} documents")

    # Print cardinality statistics
    if cardinality_stats:
        print(f"\n📊 Cardinality Statistics:")
        print(f"  Total documents: {cardinality_stats['total_documents']}")
        print(f"  Unique combinations (chain, merchant_id, product_type, product_item, order_num, order_status, city): {cardinality_stats['unique_combinations']}")
        print(f"\n  Individual field cardinalities:")
        print(f"    - Unique chains: {cardinality_stats['unique_chains']}")
        print(f"    - Unique merchant IDs: {cardinality_stats['unique_merchant_ids']}")
        print(f"    - Unique product types: {cardinality_stats['unique_product_types']}")
        print(f"    - Unique product items: {cardinality_stats['unique_product_items']}")
        print(f"    - Unique order numbers: {cardinality_stats['unique_order_nums']}")
        print(f"    - Unique order statuses: {cardinality_stats['unique_order_statuses']}")
        print(f"    - Unique cities: {cardinality_stats['unique_cities']}")

    # Show sample document
    sample = collection.find_one()
    if sample:
        print(f"\nSample document:")
        import json
        print(json.dumps(sample, indent=2, default=str))

if __name__ == "__main__":
    main()

