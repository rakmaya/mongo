# Transaction Data Generator & Loader

This directory contains tools to generate and load synthetic merchant transaction data into MongoDB for testing HCIndex functionality.

## Files

- **generate_transactions.py** - Generates synthetic transaction data
- **load_transactions.py** - Loads transactions into MongoDB
- **chains.csv** - Retail chain definitions
- **merchants.csv** - Merchant locations
- **products.csv** - Product catalog
- **transactions.csv** - Generated transaction data (output)

## Quick Start

### 1. Generate Transaction Data

```bash
cd src/mongo/db/exec/timeseries/hcindex/tssim
python3 generate_transactions.py
```

This creates `transactions.csv` with ~50,000 transactions over 30 minutes.

### 2. Load into MongoDB (Traditional)

```bash
python3 load_transactions.py
```

Creates a traditional timeseries collection and inserts all transactions.

### 3. Load into MongoDB (HCIndex-Enabled)

```bash
python3 load_transactions.py --hcindex
```

Creates an HCIndex-enabled timeseries collection for testing compression.

## Options

### generate_transactions.py

- `WINDOW_MINUTES` - Duration of transaction window (default: 30)
- `ORDERS_PER_SEC_MIN/MAX` - Orders per second range (default: 20-30)
- `FAILED_PROB` - Probability of failed transactions (default: 0.03)
- `RANDOM_SEED` - Seed for reproducibility (default: 42)

### load_transactions.py

```
--hcindex              Enable HCIndex for collection
--db DATABASE          Database name (default: metrics)
--collection NAME      Collection name (default: transactions)
--uri URI              MongoDB URI (default: mongodb://127.0.0.1:27017)
--csv PATH             Path to transactions.csv (default: transactions.csv)
```

## Data Schema

Each transaction document contains:

```javascript
{
  transaction_time: ISODate("2024-01-01T12:34:56.789Z"),
  metadata: {
    chain: "Walmart",
    merchant_id: "M001",
    product_type: "Electronics",
    product_item: "Laptop",
    order_num: "42",
    order_status: "COMPLETE",
    city: "New York"
  },
  quantity: 2,
  price: 1299.99
}
```

**Note:** `order_num` and `order_status` are stored as strings in metadata to increase cardinality for HCIndex testing.

## Testing HCIndex

### Compare Traditional vs HCIndex

```bash
# Load traditional collection
python3 load_transactions.py --collection transactions_traditional

# Load HCIndex collection
python3 load_transactions.py --hcindex --collection transactions_hcindex

# Query both and compare performance
mongosh
> use metrics
> db.transactions_traditional.find({metadata.chain: "Walmart"}).count()
> db.transactions_hcindex.find({metadata.chain: "Walmart"}).count()
```

## Data Characteristics

- **Time Window**: 30 minutes
- **Total Transactions**: ~50,000
- **Unique Chains**: Multiple (from chains.csv)
- **Unique Merchants**: Multiple (from merchants.csv)
- **Unique Products**: Multiple (from products.csv)
- **High Cardinality**: (chain, merchant, product) combinations
- **Realistic Distribution**: Weighted by chain traffic, quantity distribution

## Performance Notes

- Generation: ~1-2 seconds
- Loading (traditional): ~5-10 seconds
- Loading (HCIndex): ~5-10 seconds (with metadata encoding)
- Query performance: HCIndex should be 10-100x faster for metadata filters

