import logging
import duckdb
from research_graph.processing import writers


logger = logging.getLogger(__name__)


REFERENCE_TABLES = {
    "concepts": {
        "id": "concept_id",
        "name": "concept_name"},
    "institutions": {
        "id": "institution_id",
        "name": "institution_name"},
    "venues": {
        "id": "venue_id",
         "name": "venue_name"},
    "authors": {
        "id": "author_id",
        "name": "author_name"},
}


def run(context):
    config = context.config

    shards_root = config["shards_root"]
    tables_root = config["tables_root"]

    tables_root.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    try:
        for table_name, columns in REFERENCE_TABLES.items():
            logger.info(f"Deduplicating {table_name}")

            table_name_root = shards_root / table_name

            files = [
                file for file in table_name_root.glob("*.parquet")
                if writers.extract_date(file.name) is not None
            ]

            input_paths = [file.as_posix() for file in files]
            input_paths_sql = ", ".join(f"'{path}'" for path in input_paths)
            output_path = (tables_root / f"{table_name}.parquet").as_posix()

            if not files:
                logger.warning(f"No parquet files found for table: {table_name}, skipping...")
                continue
            
            id_column = columns["id"]
            name_column = columns["name"]

            query = f"""
            COPY (
                SELECT * EXCLUDE(rn)
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY {id_column}
                            ORDER BY 
                                filename DESC,
                                {name_column} DESC
                        ) AS rn
                    FROM read_parquet(
                        [{input_paths_sql}],
                        filename=true,
                        union_by_name=true
                    )
                )
                WHERE rn = 1
            )
            TO '{output_path}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD
            )
            """

            con.execute(query)

            row_count = con.execute(f"""
                SELECT COUNT(*)
                FROM read_parquet('{output_path}')
            """).fetchone()[0]

            if row_count == 0:
                raise ValueError(f"Deduplicated output for {table_name} is empty")

            logger.info(f"Wrote {row_count:,} rows to {output_path}")

    finally:
        con.close()