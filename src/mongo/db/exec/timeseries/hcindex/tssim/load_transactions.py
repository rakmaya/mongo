#!/usr/bin/env python3
"""
Load transactions.csv into MongoDB as a timeseries collection.

Usage:
    python3 load_transactions.py [--hcindex] [--db DATABASE] [--collection COLLECTION]

Options:
    --hcindex       Create collection with HCIndex enabled (default: disabled)
    --db DATABASE   Database name (default: metrics)
    --collection    Collection name (default: transactions)
"""

import csv
import sys
import argparse
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
    parser.add_argument("--rows", type=int, default=3, help="Number of rows to load (default: 100, 0 = all rows)")
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
        #timeseries_config["granularity"] = "HOURLY"

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

def load_transactions(csv_path, db, collection_name, max_rows=0):
    """Load transactions from CSV into MongoDB.

    Args:
        csv_path: Path to transactions.csv
        db: MongoDB database
        collection_name: Collection name
        max_rows: Maximum rows to load (0 = all rows)
    """
    collection = db[collection_name]

    documents = []
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

    print(f"Loaded {len(documents)} documents from {csv_path}")
    
    # Insert in batches
    batch_size = 1000
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i+batch_size]
        try:
            result = collection.insert_many(batch)
            print(f"  Inserted batch {i//batch_size + 1}: {len(result.inserted_ids)} documents")
        except Exception as e:
            print(f"✗ Error inserting batch: {e}")
            return False
    
    print(f"✓ Successfully inserted {len(documents)} transactions")
    return True

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
    if not load_transactions(args.csv, db, args.collection, args.rows):
        sys.exit(1)
    
    # Print summary
    collection = db[args.collection]
    count = collection.count_documents({})
    print(f"\n✓ Collection '{args.collection}' now contains {count} documents")
    
    # Show sample document
    sample = collection.find_one()
    if sample:
        print(f"\nSample document:")
        import json
        print(json.dumps(sample, indent=2, default=str))

if __name__ == "__main__":
    main()

