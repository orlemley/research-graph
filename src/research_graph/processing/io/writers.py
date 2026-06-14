import uuid
import shutil
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import re
import os
import json


logger = logging.getLogger(__name__)


class BatchedParquetWriter:
    def __init__(self, filename, schema, compression=None, batch_size=10000):
        self.filename = filename
        self.schema = schema
        self.batch_size = batch_size
        self.compression = compression or "zstd"
        
        self.buffer = []
        self.writer = None
        
        self.temp_file = self.filename.parent / f"{self.filename.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"
        self.filename.parent.mkdir(parents=True, exist_ok=True)

        if self.schema is not None:
            self.fieldnames = schema.names
            self.writer = pq.ParquetWriter(self.temp_file, self.schema, compression=self.compression)

    
    def write(self, row):
        if row is None:
            return
        
        self.buffer.append(row)

        if len(self.buffer) >= self.batch_size:
            self.flush()


    def flush(self):
        if not self.buffer:
            return

        if self.schema is None:
            columns = {
            key: [row.get(key, None) for row in self.buffer]
            for key in self.buffer[0].keys()
            }

            table = pa.table(columns)
            self.schema = table.schema
            self.fieldnames = self.schema.names
            self.writer = pq.ParquetWriter(self.temp_file, self.schema, compression=self.compression, use_dictionary=False)

        arrays = []
        for field in self.schema:
            values = [row.get(field.name, None) for row in self.buffer]
            arrays.append(pa.array(values, type=field.type))
        table = pa.Table.from_arrays(arrays, schema=self.schema)
               
        self.writer.write_table(table)
        self.buffer.clear()


    def close(self):
        self.flush()

        if self.writer is None:
            return

        self.writer.close()
        self.writer = None

        if self.temp_file.exists():
            os.replace(self.temp_file, self.filename)


def extract_date(name):
    match = re.search(r"(\d{4}-\d{2}-\d{2})", name)
    return match.group(1) if match else None


#Do not use on untrusted data
def get_sql_list(list):
    sql_list = ", ".join(f"'{item}'" for item in list)
    return sql_list


def get_sql_input_paths(name, sub_shards_root):
    files = [
        file for file in sub_shards_root.glob("*.parquet")
        if extract_date(file.name) is not None
    ]

    if not files:
        logger.warning(f"No {name} parquet files found")
        return None

    input_paths = [file.as_posix() for file in files]
    input_paths_sql = ", ".join(f"'{path}'" for path in input_paths)

    logger.info(f"Found {len(files):,} {name} shards")
    
    return input_paths_sql


def get_sql_output_paths(output_root):
    files = [file for file in output_root.rglob("*.parquet")]

    if not files:
        logger.warning(f"No output parquet files found")
        return None

    output_paths = [file.as_posix() for file in files]
    output_paths_sql = ", ".join(f"'{path}'" for path in output_paths)

    logger.info(f"Found {len(files):,} output files")
    
    return output_paths_sql


def is_valid_parquet(path):
    try:
        if path.stat().st_size < 100:
            return False

        pq.read_metadata(path)
        return True
    except Exception:
        return False
    

def check_required_parquet_files(root, name):
    if not root.exists():
        raise FileNotFoundError(f"Required input folder does not exist: {name} at {root}")

    files = list(root.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"Required input folder has no parquet files: {name} at {root}")

    return files
    

def reset_folder(path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def write_failure_record(path, record):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record))
        f.write("\n")
    

def cleanup_shard_outputs(paths):
    for path in paths.values():
        if path.exists():
            if not is_valid_parquet(path):
                logger.warning(f"Corrupt parquet detected: {path}")
            logger.warning(f"Deleting {path}")
            try:
                path.unlink(missing_ok=True)
            except Exception:
                logger.exception(f"Failed deleting {path}")


def cleanup_temp_files(root):
    removed = 0
    for temp_file in root.rglob("*.tmp.parquet"):
        try:
            temp_file.unlink()
            removed += 1
        except Exception as e:
            logger.exception(f"Failed to remove {temp_file}: {e}")

    logger.info(f"Cleanup complete: removed {removed} temp files")