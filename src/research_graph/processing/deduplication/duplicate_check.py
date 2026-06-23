from research_graph.processing.io import writers


def check_duplicate_keys(output_root, deduplication_key_sql, con):
    files = writers.get_sql_output_paths(output_root)
    duplicate_count = con.execute(f"""
        SELECT COUNT(*)
        FROM (
            SELECT {deduplication_key_sql}
            FROM read_parquet([{files}])
            GROUP BY {deduplication_key_sql}
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    return duplicate_count