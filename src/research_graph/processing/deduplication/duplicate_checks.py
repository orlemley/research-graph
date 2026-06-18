from research_graph.processing.io import writers


def check_duplicate_ids(output_root, id_column, con):
    files = writers.get_sql_output_paths(output_root)
    duplicate_count = con.execute(f"""
        SELECT COUNT(*)
        FROM (
            SELECT
                {id_column},
                COUNT(*) AS c
            FROM read_parquet(
                [{files}]
            )
            GROUP BY {id_column}
            HAVING c > 1
        )
    """).fetchone()[0]

    return duplicate_count


def check_duplicate_rows(output_root, con):
    files = writers.get_sql_output_paths(output_root)
    duplicate_count = con.execute(f"""
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM read_parquet(
                [{files}]
            )
            GROUP BY *
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    return duplicate_count