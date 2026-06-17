import logging
import json
import shutil
import psutil
import time
import sys
import subprocess
from research_graph.processing import writers
from research_graph.processing import tables_info


logger = logging.getLogger(__name__)


def run_worker(action, kind, table_name, start_bucket=None, end_bucket=None):
    command = [
        sys.executable,
        "-m",
        "research_graph.processing.deduplication.deduplication_worker",
        "--action", action,
        "--kind", kind,
        "--table", table_name,
    ]

    if start_bucket is not None:
        command.extend(["--start-bucket", str(start_bucket)])

    if end_bucket is not None:
        command.extend(["--end-bucket", str(end_bucket)])

    logger.info("Launching worker: %s", " ".join(command))
    subprocess.run(command, check=True)


def run(context):
    config = context.config

    shards_root = config["shards_root"]
    tables_root = config["tables_root"]
    temp_buckets_root = config["temp_buckets_root"]
    
    tables_root.mkdir(parents=True, exist_ok=True)
    temp_buckets_root.mkdir(parents=True, exist_ok=True)

    proc = psutil.Process()

    done_checks = []

    for name, values in tables_info.REFERENCE_TABLES.items():
        id_column = values['id']
        buckets_count = values['buckets_count']
        output_root = tables_root / name
        sub_buckets_root = temp_buckets_root / name
        sub_shards_root = shards_root / name

        expected = {
            "id_column": id_column,
            "sorting_column": values["sortby"],
            "buckets_count": buckets_count
        }
        if(sub_buckets_root / "done.json").exists():
            done_file = sub_buckets_root / "done.json"
            with done_file.open() as f:
                actual = json.load(f)
        else:
            actual = None

        shards_exist = True
        if not ((sub_buckets_root / "done.json").exists() and (actual == expected)) and not (output_root / ".done").exists():
            start_time = time.perf_counter()
            rss_before = proc.memory_info().rss / 1024**3
            io_before = proc.io_counters()
            shards_exist = any(sub_shards_root.glob("*.parquet"))
            logger.info(f"Running partition subprocess for {name}")
            run_worker("partition", "reference", name)
            if shards_exist and (sub_buckets_root / "done.json").exists():
                logger.info(f"Completed {name} partitioning")
                elapsed = time.perf_counter() - start_time
                rss_after = proc.memory_info().rss / 1024**3
                io_after = proc.io_counters()
                read_gb = (io_after.read_bytes - io_before.read_bytes) / 1024**3
                write_gb = (io_after.write_bytes - io_before.write_bytes) / 1024**3
                logger.info(f"rss_before={rss_before:.2f}GB")
                logger.info(f"rss_after={rss_after:.2f}GB")
                logger.info(f"read_gb_per_sec={read_gb / elapsed:.2f}")
                logger.info(f"write_gb_per_sec={write_gb / elapsed:.2f}")

        if(sub_buckets_root / "done.json").exists():
            done_file = sub_buckets_root / "done.json"
            with done_file.open() as f:
                actual = json.load(f)
        else:
            actual = None

        if (sub_buckets_root / "done.json").exists() and (actual == expected):
            chunk_size = config.get("deduplication_chunk_size", 4)
            for start_bucket in range(0, buckets_count, chunk_size):
                end_bucket = min(start_bucket + chunk_size, buckets_count)
                chunk = (start_bucket + chunk_size) / chunk_size
                if not all(((output_root / f"{name}_{bucket}.parquet").exists() and writers.is_valid_parquet(output_root / f"{name}_{bucket}.parquet")) for bucket in range(start_bucket, end_bucket)):
                    start_time = time.perf_counter()
                    rss_before = proc.memory_info().rss / 1024**3
                    io_before = proc.io_counters()
                    logger.info(f"Processing {name} deduplication chunk {chunk}")
                    run_worker("deduplicate-range", "reference", name, start_bucket, end_bucket)
                    if all(((output_root / f"{name}_{bucket}.parquet").exists() and writers.is_valid_parquet(output_root / f"{name}_{bucket}.parquet")) for bucket in range(start_bucket, end_bucket)):
                        logger.info(f"Finished processing {name} deduplication chunk {chunk}")
                        elapsed = time.perf_counter() - start_time
                        rss_after = proc.memory_info().rss / 1024**3
                        io_after = proc.io_counters()
                        read_gb = (io_after.read_bytes - io_before.read_bytes) / 1024**3
                        write_gb = (io_after.write_bytes - io_before.write_bytes) / 1024**3
                        logger.info(f"chunk={chunk}")
                        logger.info(f"rss_before={rss_before:.2f}GB")
                        logger.info(f"rss_after={rss_after:.2f}GB")
                        logger.info(f"read_gb_per_sec={read_gb / elapsed:.2f}")
                        logger.info(f"write_gb_per_sec={write_gb / elapsed:.2f}")
                    else:
                        logger.error(f"Worker finished but {name} chunk {chunk} containing buckets {start_bucket}-{end_bucket - 1} is not fully processed")
                        raise RuntimeError(f"Worker finished but {name} chunk {chunk} containing buckets {start_bucket}-{end_bucket - 1} is not fully processed")

            logger.info(f"Running check-duplicates subprocess for {name}")
            run_worker("check-duplicates", "reference", name)

            (output_root / ".done").touch(exist_ok=True)
            logger.info(f"Completed all deduplication for {name} table")
            logger.info(f"Deleting {name} temporary buckets folder")

            if (output_root / ".done").exists():
                shutil.rmtree(sub_buckets_root)

        elif not shards_exist:
            logger.warning(f"Skipping {name} table deduplication, no shards exist")
            continue
        elif (output_root / ".done").exists():
            pass
        else:
            logger.error(f"Partitioning not complete for {name} table")
            raise RuntimeError(f"Partitioning not complete for {name} table")
        
        done_checks.append((output_root / ".done").exists())
            
    for name, buckets_count in tables_info.RELATIONSHIP_TABLES.items():
        output_root = tables_root / name
        sub_buckets_root = temp_buckets_root / name

        expected = {"buckets_count": buckets_count}
        if(sub_buckets_root / "done.json").exists():
            done_file = sub_buckets_root / "done.json"
            with done_file.open() as f:
                actual = json.load(f)
        else:
            actual = None

        shards_exist = True
        if not ((sub_buckets_root / "done.json").exists() and (actual == expected)) and not (output_root / ".done").exists():
            start_time = time.perf_counter()
            rss_before = proc.memory_info().rss / 1024**3
            io_before = proc.io_counters()
            shards_exist = any(sub_shards_root.glob("*.parquet"))
            logger.info(f"Running partition subprocess for {name}")
            run_worker("partition", "relationship", name)
            if shards_exist and (sub_buckets_root / "done.json").exists():
                logger.info(f"Completed {name} partitioning")
                elapsed = time.perf_counter() - start_time
                rss_after = proc.memory_info().rss / 1024**3
                io_after = proc.io_counters()
                read_gb = (io_after.read_bytes - io_before.read_bytes) / 1024**3
                write_gb = (io_after.write_bytes - io_before.write_bytes) / 1024**3
                logger.info(f"rss_before={rss_before:.2f}GB")
                logger.info(f"rss_after={rss_after:.2f}GB")
                logger.info(f"read_gb_per_sec={read_gb / elapsed:.2f}")
                logger.info(f"write_gb_per_sec={write_gb / elapsed:.2f}")

        if(sub_buckets_root / "done.json").exists():
            done_file = sub_buckets_root / "done.json"
            with done_file.open() as f:
                actual = json.load(f)
        else:
            actual = None

        if (sub_buckets_root / "done.json").exists() and (actual == expected):
            chunk_size = config.get("deduplication_chunk_size", 4)
            for start_bucket in range(0, buckets_count, chunk_size):
                end_bucket = min(start_bucket + chunk_size, buckets_count)
                chunk = (start_bucket + chunk_size) / chunk_size
                if not all(((output_root / f"{name}_{bucket}.parquet").exists() and writers.is_valid_parquet(output_root / f"{name}_{bucket}.parquet")) for bucket in range(start_bucket, end_bucket)):
                    start_time = time.perf_counter()
                    rss_before = proc.memory_info().rss / 1024**3
                    io_before = proc.io_counters()
                    logger.info(f"Processing {name} deduplication chunk {chunk}")
                    run_worker("deduplicate-range", "relationship", name, start_bucket, end_bucket)
                    if all(((output_root / f"{name}_{bucket}.parquet").exists() and writers.is_valid_parquet(output_root / f"{name}_{bucket}.parquet")) for bucket in range(start_bucket, end_bucket)):
                        logger.info(f"Finished processing {name} deduplication chunk {chunk}")
                        elapsed = time.perf_counter() - start_time
                        rss_after = proc.memory_info().rss / 1024**3
                        io_after = proc.io_counters()
                        read_gb = (io_after.read_bytes - io_before.read_bytes) / 1024**3
                        write_gb = (io_after.write_bytes - io_before.write_bytes) / 1024**3
                        logger.info(f"chunk={chunk}")
                        logger.info(f"rss_before={rss_before:.2f}GB")
                        logger.info(f"rss_after={rss_after:.2f}GB")
                        logger.info(f"read_gb_per_sec={read_gb / elapsed:.2f}")
                        logger.info(f"write_gb_per_sec={write_gb / elapsed:.2f}")
                    else:
                        logger.error(f"Worker finished but {name} chunk {chunk} containing buckets {start_bucket}-{end_bucket - 1} is not fully processed")
                        raise RuntimeError(f"Worker finished but {name} chunk {chunk} containing buckets {start_bucket}-{end_bucket - 1} is not fully processed")
            
            logger.info(f"Running check-duplicates subprocess for {name}")
            run_worker("check-duplicates", "relationship", name)
            
            (output_root / ".done").touch(exist_ok=True)
            logger.info(f"Completed all deduplication for {name} table")
            logger.info(f"Deleting {name} temporary buckets folder")

            if (output_root / ".done").exists():
                shutil.rmtree(sub_buckets_root)

        elif not shards_exist:
            logger.warning(f"Skipping {name} table deduplication, no shards exist")
            continue
        elif (output_root / ".done").exists():
            pass
        else:
            logger.error(f"Partitioning not complete for {name} table")
            raise RuntimeError(f"Partitioning not complete for {name} table")
        
        done_checks.append((output_root / ".done").exists())

    if done_checks and all(done_checks):
        shutil.rmtree(temp_buckets_root)