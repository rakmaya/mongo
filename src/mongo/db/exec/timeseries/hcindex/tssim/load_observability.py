#!/usr/bin/env python3
"""
Load observability metrics into MongoDB as a timeseries collection.

Generates realistic observability metrics for a cloud infrastructure with dynamic
host, container, and cluster generation. Emits separate metric documents from
different subsystems (system, disk, network_rx, network_tx, application) to simulate
real-world observability pipelines.

Usage:
    python3 load_observability.py [--hcindex] [--hours 1] [--interval 10]

Options:
    --hcindex           Enable HCIndex for the collection (default: disabled)
    --db DATABASE       Database name (default: metrics)
    --collection NAME   Collection name (default: observability_hc or observability_no_hc)
    --uri URI           MongoDB connection URI (default: mongodb://127.0.0.1:27017)
    --csv PATH          Path to observability_metadata.csv (default: observability_metadata.csv)
    --hours N           Number of hours to simulate (default: 1)
    --interval N        Metric interval in seconds (default: 10)
    --insert-mode MODE  Insert mode: 'batch' or 'single' (default: batch)
    --min-replicas N    Minimum replicas per service (default: 3)
    --max-replicas N    Maximum replicas per service (default: 10)
    --restart-prob P    Probability of container restart per hour (default: 0.05)
    --num-tables N      Number of database tables to simulate (default: 25)
    --dry-run           Generate metrics but don't insert into MongoDB (just print sample)
"""

import csv
import sys
import argparse
import time
import random
import string
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import OperationFailure

def parse_args():
    parser = argparse.ArgumentParser(description="Load observability metrics into MongoDB timeseries collection")
    parser.add_argument("--hcindex", action="store_true", help="Enable HCIndex for the collection")
    parser.add_argument("--db", default="metrics", help="Database name (default: metrics)")
    parser.add_argument("--collection", default=None, help="Collection name (default: observability_hc or observability_no_hc)")
    parser.add_argument("--uri", default="mongodb://127.0.0.1:27017", help="MongoDB connection URI")
    parser.add_argument("--csv", default="observability_metadata.csv", help="Path to observability_metadata.csv")
    parser.add_argument("--hours", type=float, default=1.0, help="Number of hours to simulate (default: 1)")
    parser.add_argument("--interval", type=int, default=10, help="Metric interval in seconds (default: 10)")
    parser.add_argument("--insert-mode", choices=["batch", "single"], default="batch",
                        help="Insert mode: 'batch' (insert_many) or 'single' (insert_one) (default: batch)")
    parser.add_argument("--min-replicas", type=int, default=3, help="Minimum replicas per service (default: 3)")
    parser.add_argument("--max-replicas", type=int, default=10, help="Maximum replicas per service (default: 10)")
    parser.add_argument("--restart-prob", type=float, default=0.05, help="Container restart probability per hour (default: 0.05)")
    parser.add_argument("--dry-run", action="store_true", help="Generate metrics but don't insert into MongoDB (just print sample)")
    parser.add_argument("--num-tables", type=int, default=25, help="Number of database tables to simulate (default: 25)")
    args = parser.parse_args()

    # Set default collection name based on hcindex flag
    if args.collection is None:
        args.collection = "observability_hc" if args.hcindex else "observability_no_hc"

    return args

def random_hex(length=8):
    """Generate random hexadecimal string."""
    return ''.join(random.choices(string.hexdigits.lower(), k=length))

def generate_table_names(num_tables):
    """Generate generic table names like table-000, table-001, etc."""
    return [f"table-{i:03d}" for i in range(num_tables)]

def generate_cluster_name(region, environment):
    """Generate cluster name from region and environment."""
    # Extract region suffix (e.g., us-east-1 -> 01, us-west-2 -> 02)
    region_map = {
        'us-east-1': '01',
        'us-west-2': '02',
        'eu-west-1': '03'
    }
    suffix = region_map.get(region, '99')

    env_short = {
        'production': 'prod',
        'staging': 'staging',
        'development': 'dev'
    }

    return f"{env_short.get(environment, environment)}-cluster-{suffix}"

def generate_hosts(csv_path, min_replicas, max_replicas):
    """Generate host configurations with random scaling.

    Args:
        csv_path: Path to observability_metadata.csv
        min_replicas: Minimum replicas per service
        max_replicas: Maximum replicas per service

    Returns:
        List of host configurations with dynamically generated host, container, cluster
    """
    hosts = []

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)

        for base_config in reader:
            # Random number of replicas per service (simulates auto-scaling)
            num_replicas = random.randint(min_replicas, max_replicas)

            # Extract service short name for host naming
            service = base_config['service']
            service_short = service.split('-')[0][:3]  # e.g., "api" from "api-gateway"

            for replica_idx in range(num_replicas):
                host_config = {
                    # Static fields from CSV
                    'region': base_config['region'],
                    'availability_zone': base_config['availability_zone'],
                    'environment': base_config['environment'],
                    'service': service,
                    'pod': base_config['pod'],
                    'namespace': base_config['namespace'],
                    'instance_type': base_config['instance_type'],

                    # Dynamically generated fields
                    'host': f"{service_short}-{replica_idx:02d}",
                    'container': f"{base_config['pod']}-{random_hex(8)}",
                    'cluster': generate_cluster_name(base_config['region'], base_config['environment']),

                    # Replica info (for tracking)
                    'replica_index': replica_idx,
                    'total_replicas': num_replicas
                }
                hosts.append(host_config)

    return hosts

def generate_cpu_percent(host, timestamp, base_time):
    """Generate realistic CPU percentage with patterns."""
    # Base CPU varies by service type
    base_cpu = {
        'api-gateway': 35.0,
        'user-service': 45.0,
        'order-service': 50.0,
        'payment-service': 60.0,
        'postgres': 70.0,
        'redis': 25.0,
        'rabbitmq': 30.0,
        'nginx': 20.0,
        'background-jobs': 55.0,
        'email-service': 40.0,
        'notification-service': 40.0,
        'prometheus': 50.0,
        'grafana': 35.0,
        'elasticsearch': 65.0,
        'opensearch': 60.0,
        'ml-inference': 80.0,
    }.get(host['service'], 40.0)

    # Time-of-day pattern (higher during business hours)
    hour = timestamp.hour
    if 9 <= hour <= 17:
        time_multiplier = 1.3
    elif 0 <= hour <= 6:
        time_multiplier = 0.7
    else:
        time_multiplier = 1.0

    # Add some randomness and periodic variation
    seconds_since_start = (timestamp - base_time).total_seconds()
    periodic = 10 * (1 + 0.5 * (seconds_since_start % 300) / 300)  # 5-minute cycle
    noise = random.uniform(-5, 5)

    cpu = base_cpu * time_multiplier + periodic + noise
    return max(5.0, min(95.0, cpu))  # Clamp between 5-95%

def generate_memory_bytes(host, instance_type):
    """Generate memory usage based on instance type."""
    # Memory capacity by instance type (in GB)
    memory_capacity = {
        't3.small': 2,
        't3.medium': 4,
        't3.large': 8,
        't3.xlarge': 16,
        't3.2xlarge': 32,
        'r5.large': 16,
        'r5.xlarge': 32,
        'r5.2xlarge': 64,
        'r5.4xlarge': 128,
        'g4dn.xlarge': 16,
    }.get(instance_type, 8)

    # Memory usage percentage (varies by service)
    usage_percent = random.uniform(40, 85)

    # Convert to bytes
    memory_bytes = int(memory_capacity * 1024 * 1024 * 1024 * usage_percent / 100)
    return memory_bytes

def generate_disk_io(host):
    """Generate disk I/O bytes."""
    # Database and logging services have higher disk I/O
    if host['namespace'] in ['database', 'logging', 'search']:
        base_io = random.uniform(5_000_000, 50_000_000)  # 5-50 MB/s
    else:
        base_io = random.uniform(100_000, 5_000_000)  # 100KB-5MB/s

    return int(base_io)

def generate_network(host):
    """Generate network rx/tx bytes."""
    # API gateways and services have higher network traffic
    if host['namespace'] in ['default', 'backend', 'ingress']:
        base_network = random.uniform(1_000_000, 10_000_000)  # 1-10 MB/s
    else:
        base_network = random.uniform(100_000, 1_000_000)  # 100KB-1MB/s

    rx_bytes = int(base_network * random.uniform(0.8, 1.2))
    tx_bytes = int(base_network * random.uniform(0.8, 1.2))

    return rx_bytes, tx_bytes

def generate_request_metrics(host):
    """Generate request count and response time for API services."""
    if host['namespace'] in ['default', 'backend', 'ingress']:
        request_count = random.randint(100, 1000)
        response_time_ms = random.uniform(50, 500)
        error_count = random.randint(0, int(request_count * 0.02))  # 0-2% error rate
    else:
        request_count = 0
        response_time_ms = 0
        error_count = 0

    return request_count, response_time_ms, error_count

def generate_database_metrics():
    """Generate database-specific metrics for postgres service."""
    read_request_count = random.randint(100, 200)
    write_request_count = random.randint(20, 50)
    rows_read_count = random.randint(5000, 40000)
    rows_written_count = random.randint(100, 1000)

    return read_request_count, write_request_count, rows_read_count, rows_written_count

def should_restart_container(restart_prob, interval_seconds):
    """Determine if container should restart based on probability."""
    # Convert hourly probability to per-interval probability
    prob_per_interval = restart_prob * (interval_seconds / 3600.0)
    return random.random() < prob_per_interval

def create_timeseries_collection(db, collection_name, use_hcindex=False):
    """Create a timeseries collection with optional HCIndex."""
    try:
        db.drop_collection(collection_name)
        print(f"Dropped existing collection: {collection_name}")
    except Exception as e:
        print(f"Note: {e}")

    # Build create collection command
    timeseries_config = {
        "timeField": "timestamp",
        "metaField": "metadata"
    }

    if use_hcindex:
        timeseries_config["useHCIndex"] = True
        timeseries_config["hcindexOptions"] = {
            "period": "hour",
            "frequency": 1,
            # Exclude high-cardinality fields that change frequently
            "excludedColumns": ["host", "container", "pod"]
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

def generate_and_load_metrics(args, db, collection_name, hosts, table_names):
    """Generate and load metrics for all hosts over time.

    Generates separate documents for different metric types to simulate
    real-world observability where different subsystems emit different metrics:
    - System metrics: cpu_percent, memory_bytes (from node_exporter)
    - Disk metrics: disk_io_bytes (from disk subsystem)
    - Network RX metrics: network_rx_bytes (from network subsystem)
    - Network TX metrics: network_tx_bytes (from network subsystem)
    - Application metrics: request_count, response_time_ms, error_count (from app)
    - Database metrics: read_request_count, write_request_count, rows_read_count,
                       rows_written_count (from postgres, per table)
    """

    collection = db[collection_name] if db is not None else None

    # Calculate time parameters
    start_time = datetime.now().replace(microsecond=0)
    interval = timedelta(seconds=args.interval)
    num_intervals = int((args.hours * 3600) / args.interval)

    print(f"\n📊 Generating metrics:")
    print(f"  Hosts: {len(hosts)}")
    print(f"  Time range: {args.hours} hours")
    print(f"  Interval: {args.interval} seconds")
    print(f"  Metric streams: 6 (system, disk, network_rx, network_tx, application, database)")
    print(f"  Note: Database (postgres) service generates {len(table_names)} docs per interval (one per table)")
    # Rough estimate - not exact since application and database metrics are conditional
    print(f"  Estimated data points: ~{len(hosts) * num_intervals * 5:,}+")

    # Track statistics
    unique_hosts = set()
    unique_containers = set()
    unique_clusters = set()
    unique_regions = set()
    unique_azs = set()
    unique_environments = set()
    unique_services = set()
    unique_pods = set()
    unique_namespaces = set()
    unique_instance_types = set()
    unique_metric_types = set()

    # Count unique combinations (not storing actual tuples to save memory)
    seen_combinations = set()  # Temporary for deduplication within generation

    documents_generated = 0
    metric_type_counts = {
        'system': 0,
        'disk': 0,
        'network_rx': 0,
        'network_tx': 0,
        'application': 0,
        'database': 0
    }

    # Generate metrics
    start_perf_time = time.perf_counter()
    documents_by_interval = []  # 2-level list: each inner list is documents for one interval

    for interval_idx in range(num_intervals):
        timestamp = start_time + (interval_idx * interval)
        interval_documents = []  # Documents for this specific interval

        for host in hosts:
            # Simulate container restarts
            if should_restart_container(args.restart_prob, args.interval):
                old_container = host['container']
                host['container'] = f"{host['pod']}-{random_hex(8)}"
                if interval_idx % 360 == 0:  # Log every hour
                    print(f"  🔄 Container restart: {old_container} → {host['container']}")

            # Common metadata for all metric types
            common_metadata = {
                "region": host['region'],
                "availability_zone": host['availability_zone'],
                "environment": host['environment'],
                "service": host['service'],
                "pod": host['pod'],
                "namespace": host['namespace'],
                "instance_type": host['instance_type'],
                "host": host['host'],
                "container": host['container'],
                "cluster": host['cluster']
            }

            # Track unique values for each dimension
            unique_hosts.add(host['host'])
            unique_containers.add(host['container'])
            unique_clusters.add(host['cluster'])
            unique_regions.add(host['region'])
            unique_azs.add(host['availability_zone'])
            unique_environments.add(host['environment'])
            unique_services.add(host['service'])
            unique_pods.add(host['pod'])
            unique_namespaces.add(host['namespace'])
            unique_instance_types.add(host['instance_type'])

            # Generate separate documents for each metric type

            # 1. System metrics (CPU + Memory) - from node_exporter/system agent
            cpu_percent = generate_cpu_percent(host, timestamp, start_time)
            memory_bytes = generate_memory_bytes(host, host['instance_type'])
            metric_type = "system"
            unique_metric_types.add(metric_type)
            interval_documents.append({
                "timestamp": timestamp,
                "metadata": {**common_metadata, "metric_type": metric_type},
                "cpu_percent": cpu_percent,
                "memory_bytes": memory_bytes
            })
            metric_type_counts['system'] += 1
            documents_generated += 1
            combo = (host['region'], host['availability_zone'], host['environment'],
                    host['service'], host['pod'], host['namespace'], host['instance_type'],
                    host['host'], host['container'], host['cluster'], metric_type)
            seen_combinations.add(combo)

            # 2. Disk I/O metrics - from disk subsystem
            disk_io_bytes = generate_disk_io(host)
            metric_type = "disk"
            unique_metric_types.add(metric_type)
            interval_documents.append({
                "timestamp": timestamp,
                "metadata": {**common_metadata, "metric_type": metric_type},
                "disk_io_bytes": disk_io_bytes
            })
            metric_type_counts['disk'] += 1
            documents_generated += 1
            combo = (host['region'], host['availability_zone'], host['environment'],
                    host['service'], host['pod'], host['namespace'], host['instance_type'],
                    host['host'], host['container'], host['cluster'], metric_type)
            seen_combinations.add(combo)

            # 3. Network RX metrics - from network subsystem (ingress)
            network_rx_bytes, network_tx_bytes = generate_network(host)
            metric_type = "network_rx"
            unique_metric_types.add(metric_type)
            interval_documents.append({
                "timestamp": timestamp,
                "metadata": {**common_metadata, "metric_type": metric_type},
                "network_rx_bytes": network_rx_bytes
            })
            metric_type_counts['network_rx'] += 1
            documents_generated += 1
            combo = (host['region'], host['availability_zone'], host['environment'],
                    host['service'], host['pod'], host['namespace'], host['instance_type'],
                    host['host'], host['container'], host['cluster'], metric_type)
            seen_combinations.add(combo)

            # 4. Network TX metrics - from network subsystem (egress)
            metric_type = "network_tx"
            unique_metric_types.add(metric_type)
            interval_documents.append({
                "timestamp": timestamp,
                "metadata": {**common_metadata, "metric_type": metric_type},
                "network_tx_bytes": network_tx_bytes
            })
            metric_type_counts['network_tx'] += 1
            documents_generated += 1
            combo = (host['region'], host['availability_zone'], host['environment'],
                    host['service'], host['pod'], host['namespace'], host['instance_type'],
                    host['host'], host['container'], host['cluster'], metric_type)
            seen_combinations.add(combo)

            # 5. Application metrics - from application instrumentation
            # Only for services that handle requests
            if host['namespace'] in ['default', 'backend', 'ingress']:
                metric_type = "application"
                unique_metric_types.add(metric_type)
                request_count, response_time_ms, error_count = generate_request_metrics(host)
                interval_documents.append({
                    "timestamp": timestamp,
                    "metadata": {**common_metadata, "metric_type": metric_type},
                    "request_count": request_count,
                    "response_time_ms": response_time_ms,
                    "error_count": error_count
                })
                metric_type_counts['application'] += 1
                documents_generated += 1
                combo = (host['region'], host['availability_zone'], host['environment'],
                        host['service'], host['pod'], host['namespace'], host['instance_type'],
                        host['host'], host['container'], host['cluster'], metric_type)
                seen_combinations.add(combo)

            # 6. Database metrics - from postgres service (one document per table)
            # Only for postgres service
            if host['service'] == 'postgres':
                metric_type = "database"
                unique_metric_types.add(metric_type)
                # Generate metrics for each table
                for table_name in table_names:
                    read_req_count, write_req_count, rows_read, rows_written = generate_database_metrics()
                    # Add table_name to metadata
                    db_metadata = {**common_metadata, "metric_type": metric_type, "table_name": table_name}
                    interval_documents.append({
                        "timestamp": timestamp,
                        "metadata": db_metadata,
                        "read_request_count": read_req_count,
                        "write_request_count": write_req_count,
                        "rows_read_count": rows_read,
                        "rows_written_count": rows_written
                    })
                    metric_type_counts['database'] += 1
                    documents_generated += 1
                    combo = (host['region'], host['availability_zone'], host['environment'],
                            host['service'], host['pod'], host['namespace'], host['instance_type'],
                            host['host'], host['container'], host['cluster'], metric_type, table_name)
                    seen_combinations.add(combo)

        # Add this interval's documents to the main list
        documents_by_interval.append(interval_documents)

        # Progress indicator
        if (interval_idx + 1) % 36 == 0:  # Every 6 minutes (at 10s interval)
            elapsed_hours = (interval_idx + 1) * args.interval / 3600
            print(f"  Generated {elapsed_hours:.2f}h of metrics ({documents_generated:,} documents)...")

    print(f"\n✓ Generated {documents_generated:,} metric documents")
    print(f"  Breakdown by metric type:")
    for metric_type, count in sorted(metric_type_counts.items()):
        print(f"    - {metric_type}: {count:,} documents")

    # Insert based on mode or dry-run
    if args.dry_run:
        print(f"\n🔍 DRY RUN MODE - Not inserting into MongoDB")
        print(f"  Would insert {documents_generated:,} documents in {len(documents_by_interval)} interval batches")
        print(f"  Avg documents per interval: {documents_generated / len(documents_by_interval):.1f}")
        print(f"\n📄 Sample documents (first 5 from first interval):")
        import json
        first_interval_docs = documents_by_interval[0] if documents_by_interval else []
        for i, doc in enumerate(first_interval_docs[:5]):
            print(f"\n  Document {i+1}:")
            print(json.dumps(doc, indent=4, default=str))
        insert_time = 0
        total_time = (time.perf_counter() - start_perf_time) * 1000
        print(f"\n⏱️  Performance:")
        print(f"  Generation time: {total_time:.2f}ms")
    else:
        print(f"\n💾 Inserting metrics using {args.insert_mode} mode...")
        insert_start_time = time.perf_counter()

        if args.insert_mode == "single":
            inserted_count = 0
            total_docs = 0
            for interval_idx, interval_docs in enumerate(documents_by_interval):
                for doc in interval_docs:
                    try:
                        collection.insert_one(doc)
                        inserted_count += 1
                        total_docs += 1
                        if total_docs % 1000 == 0:
                            print(f"  Inserted {total_docs:,} documents...")
                    except Exception as e:
                        print(f"✗ Error inserting document in interval {interval_idx}: {e}")
                        return False, {}
            print(f"✓ Inserted {inserted_count:,} documents using insert_one")
        else:
            # Batch insert - one batch per interval to mimic real-world burst pattern
            total_docs = 0
            for interval_idx, interval_docs in enumerate(documents_by_interval):
                try:
                    result = collection.insert_many(interval_docs)
                    total_docs += len(result.inserted_ids)
                    if interval_idx % 36 == 0:  # Log every 6 minutes (at 10s interval)
                        elapsed_hours = (interval_idx + 1) * args.interval / 3600
                        print(f"  Inserted {total_docs:,} documents ({elapsed_hours:.2f}h of metrics, batch size: {len(interval_docs)})")
                except Exception as e:
                    print(f"✗ Error inserting batch for interval {interval_idx}: {e}")
                    return False, {}
            print(f"✓ Inserted {total_docs:,} documents in {len(documents_by_interval)} interval-based batches")
            print(f"  Avg batch size: {total_docs / len(documents_by_interval):.1f} documents per interval")

        insert_end_time = time.perf_counter()
        insert_time = (insert_end_time - insert_start_time) * 1000
        total_time = (insert_end_time - start_perf_time) * 1000

        print(f"\n⏱️  Performance:")
        print(f"  Total time: {total_time:.2f}ms")
        print(f"  Insert time: {insert_time:.2f}ms")
        print(f"  Per document: {insert_time/documents_generated:.3f}ms")

    # Prepare statistics
    stats = {
        "total_documents": documents_generated,
        "unique_regions": len(unique_regions),
        "unique_azs": len(unique_azs),
        "unique_environments": len(unique_environments),
        "unique_services": len(unique_services),
        "unique_pods": len(unique_pods),
        "unique_namespaces": len(unique_namespaces),
        "unique_instance_types": len(unique_instance_types),
        "unique_hosts": len(unique_hosts),
        "unique_containers": len(unique_containers),
        "unique_clusters": len(unique_clusters),
        "unique_metric_types": len(unique_metric_types),
        "unique_combinations": len(seen_combinations),
        "time_range_hours": args.hours,
        "interval_seconds": args.interval,
        "hosts_per_interval": len(hosts),
        "metric_type_counts": metric_type_counts
    }

    return True, stats

def main():
    args = parse_args()

    # Generate host configurations
    print(f"\n🔧 Generating host configurations from {args.csv}...")
    try:
        hosts = generate_hosts(args.csv, args.min_replicas, args.max_replicas)
        print(f"✓ Generated {len(hosts)} host configurations")
        print(f"  Replicas per service: {args.min_replicas}-{args.max_replicas}")
    except Exception as e:
        print(f"✗ Failed to generate hosts: {e}")
        sys.exit(1)

    # Generate table names
    table_names = generate_table_names(args.num_tables)
    print(f"✓ Generated {len(table_names)} table names (table-000 to table-{args.num_tables-1:03d})")

    # Skip MongoDB connection and collection creation in dry-run mode
    if args.dry_run:
        print(f"\n🔍 DRY RUN MODE - Skipping MongoDB connection")
        db = None
    else:
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

    # Generate and load metrics
    success, stats = generate_and_load_metrics(args, db, args.collection, hosts, table_names)
    if not success:
        sys.exit(1)

    # Print summary (skip count in dry-run mode)
    if not args.dry_run:
        collection = db[args.collection]
        count = collection.count_documents({})
        print(f"\n✓ Collection '{args.collection}' now contains {count:,} documents")

    # Print cardinality statistics
    if stats:
        print(f"\n📊 Cardinality Statistics:")
        print(f"  Total documents: {stats['total_documents']:,}")
        print(f"\n  Unique values per dimension:")
        print(f"    - Regions: {stats['unique_regions']}")
        print(f"    - Availability Zones: {stats['unique_azs']}")
        print(f"    - Environments: {stats['unique_environments']}")
        print(f"    - Services: {stats['unique_services']}")
        print(f"    - Pods: {stats['unique_pods']}")
        print(f"    - Namespaces: {stats['unique_namespaces']}")
        print(f"    - Instance Types: {stats['unique_instance_types']}")
        print(f"    - Hosts: {stats['unique_hosts']}")
        print(f"    - Containers: {stats['unique_containers']}")
        print(f"    - Clusters: {stats['unique_clusters']}")
        print(f"    - Metric Types: {stats['unique_metric_types']}")
        print(f"\n  Total unique combinations: {stats['unique_combinations']:,}")
        print(f"  Time range: {stats['time_range_hours']} hours")
        print(f"  Interval: {stats['interval_seconds']} seconds")
        print(f"  Hosts per interval: {stats['hosts_per_interval']}")
        print(f"\n  Documents by metric type:")
        for metric_type, count in sorted(stats['metric_type_counts'].items()):
            print(f"    - {metric_type}: {count:,}")

    # Show sample document (skip in dry-run mode as we already showed samples)
    if not args.dry_run:
        collection = db[args.collection]
        sample = collection.find_one()
        if sample:
            print(f"\n📄 Sample document:")
            import json
            print(json.dumps(sample, indent=2, default=str))

if __name__ == "__main__":
    main()
