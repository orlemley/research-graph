import logging
import argparse
import psutil
import time
import duckdb
from research_graph.processing import writers
from research_graph.processing import id_deduplication
from research_graph.processing import id_partitioning
from research_graph.processing import row_deduplication
from research_graph.processing import row_partitioning
from research_graph.processing import duplicate_checks
from research_graph.processing import tables_info
from research_graph.config import load_config
from research_graph.config import setup_logging





def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", choices=["partition", "deduplicate-range", "check-duplicates"], required=True)
    parser.add_argument("--kind", choices=["reference", "relationship"], required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--start-bucket", type=int)
    parser.add_argument("--end-bucket", type=int)
    args = parser.parse_args()

    config = load_config()
    setup_logging(config["base_path"], config["logs_path"])
    logger = logging.getLogger(__name__)

    proc = psutil.Process()

    with duckdb.connect() as con:
        con.execute("PRAGMA preserve_insertion_order=false")

        output_root = config["tables_root"] / args.table

        if args.action == "partition":
            if args.kind == "reference":
                values = tables_info.REFERENCE_TABLES[args.table]
                id_partitioning.partition_by_id(args.table, values, config, con)
            elif args.kind == "relationship":
                buckets_count = tables_info.RELATIONSHIP_TABLES[args.table]
                row_partitioning.partition_by_row(args.table, buckets_count, config, con)

        elif args.action == "deduplicate-range":
            if args.start_bucket is None or args.end_bucket is None:
                raise ValueError("dedup-range requires --start-bucket and --end-bucket")

            if args.kind == "reference":
                values = tables_info.REFERENCE_TABLES[args.table]
                start_time = time.perf_counter()
                io_before = proc.io_counters()
                for bucket in range(args.start_bucket, args.end_bucket):
                    output_path = output_root / f"{args.table}_{bucket}.parquet"
                    if not (output_path.exists() and writers.is_valid_parquet(output_path)):
                        rows_per_second = id_deduplication.deduplicate_by_id(bucket, args.table, values, config, con)
                        io_after = proc.io_counters()
                        elapsed = time.perf_counter() - start_time
                        read_gb = (io_after.read_bytes - io_before.read_bytes) / 1024**3
                        write_gb = (io_after.write_bytes - io_before.write_bytes) / 1024**3
                        logger.info(f"bucket={bucket}")
                        logger.info(f"{rows_per_second:,.2f} rows/sec")
                        logger.info(f"read_gb_per_sec={read_gb / elapsed:.2f}")
                        logger.info(f"write_gb_per_sec={write_gb / elapsed:.2f}")
            elif args.kind == "relationship":
                start_time = time.perf_counter()
                io_before = proc.io_counters()
                for bucket in range(args.start_bucket, args.end_bucket):
                    output_path = output_root / f"{args.table}_{bucket}.parquet"
                    if not (output_path.exists() and writers.is_valid_parquet(output_path)):
                        rows_per_second = row_deduplication.deduplicate_by_row(bucket, args.table, config, con)
                        io_after = proc.io_counters()
                        elapsed = time.perf_counter() - start_time
                        read_gb = (io_after.read_bytes - io_before.read_bytes) / 1024**3
                        write_gb = (io_after.write_bytes - io_before.write_bytes) / 1024**3
                        logger.info(f"bucket={bucket}")
                        logger.info(f"{rows_per_second:,.2f} rows/sec")
                        logger.info(f"read_gb_per_sec={read_gb / elapsed:.2f}")
                        logger.info(f"write_gb_per_sec={write_gb / elapsed:.2f}")

        elif args.action == "check-duplicates":
            if args.kind == "reference":
                values = tables_info.REFERENCE_TABLES[args.table]
                id_column = values["id"]
                logger.info(f"Checking for duplicate {id_column}s across all {args.table} outputs")
                duplicate_count = duplicate_checks.check_duplicate_ids(output_root, id_column, con)
                if duplicate_count > 0:
                    logger.error(f"Found {duplicate_count:,} duplicate {id_column}s in {args.table} output")
                    raise ValueError(f"Found {duplicate_count:,} duplicate {id_column}s in {args.table} output")
                else:
                    logger.info(f"Found {duplicate_count:,} duplicate {id_column}s in {args.table} output")
            elif args.kind == "relationship":
                logger.info(f"Checking for duplicate rows across all {args.table} outputs")
                duplicate_count = duplicate_checks.check_duplicate_rows(output_root, con)
                if duplicate_count > 0:
                    logger.error(f"Found {duplicate_count:,} duplicate rows in {args.table} output")
                    raise ValueError(f"Found {duplicate_count:,} duplicate rows in {args.table} output")
                else:
                    logger.info(f"Found {duplicate_count:,} duplicate rows in {args.table} output")

        else:
            logger.error(f"Unknown action: {args.action}")
            raise ValueError(f"Unknown action: {args.action}")
        

if __name__ == "__main__":
    main()