import logging
import re
import shutil
import duckdb
from research_graph.processing.io import writers
from research_graph.processing.graph_filtering import seed_concepts
from research_graph.processing.graph_filtering import citation_edges_filter
from research_graph.processing.graph_filtering import works_filter
from research_graph.processing.graph_filtering import work_ids_filter
from research_graph.processing.graph_filtering import relationship_tables_filter
from research_graph.processing.graph_filtering import reference_tables_filter


logger = logging.getLogger(__name__)

REFERENCE_TABLES = {
    "authors": {
        "id_column": "author_id",
        "base_table": "authorships"
    },
    "concepts": {
        "id_column": "concept_id",
        "base_table": "scores"
    },
    "institutions": {
        "id_column": "institution_id",
        "base_table": "affiliations"
    },
    "venues": {
        "id_column": "venue_id",
        "base_table": "works"
    }
}

RELATIONSHIP_TABLES = ["affiliations", "authorships", "scores", "selected_scores"]


def run(context):
    config = context.config

    tables_root = config["tables_root"]
    filtered_tables_root = config["filtered_tables_root"]
    
    filtered_works_root = filtered_tables_root / "filtered_works"
    filtered_work_ids_root = filtered_tables_root / "filtered_work_ids"
    filtered_citation_edges_root = filtered_tables_root / "filtered_citation_edges"
    temp_scores_root = filtered_tables_root / "temp_scores"

    works_tables_root = tables_root / "works"
    scores_tables_root = tables_root /  "scores"
    citation_edges_tables_root = tables_root /  "citation_edges"

    filtered_tables_root.mkdir(parents=True, exist_ok=True)

    concepts = config['graph_filter_concepts']
    pattern = re.compile(r"^C\d+$")
    for concept in concepts:
        if not pattern.fullmatch(concept):
            logger.error(f"Invalid graph filter concept ID: {concept}")
            raise ValueError(f"Invalid graph filter concept ID: {concept}")

    works_file_count = len([file for file in works_tables_root.glob("*.parquet")])
    scores_file_count = len([file for file in scores_tables_root.glob("*.parquet")])
    citation_edges_file_count = len([file for file in citation_edges_tables_root.glob("*.parquet")])

    con = duckdb.connect()

    try:
        temp_scores_done = False
        for file_number in range(scores_file_count):
            if (filtered_work_ids_root / ".done").exists() or ((filtered_works_root / ".done").exists() and (filtered_tables_root / "relationship_tables.done").exists()):
                temp_scores_done = True
                break
            temp_scores_output_path = temp_scores_root / f"temp_scores_{file_number}.parquet"
            if not (temp_scores_output_path.exists() and writers.is_valid_parquet(temp_scores_output_path)):
                seed_concepts.create_temp_scores(concepts, file_number, config, con)
        if not temp_scores_done and not (temp_scores_root / ".done").exists():
            (temp_scores_root / ".done").touch(exist_ok=True)
            logger.info("Completed all temp_scores filtering")

        if (temp_scores_root / ".done").exists() and not (filtered_citation_edges_root / ".done").exists():
            for file_number in range(citation_edges_file_count):
                filtered_citation_edges_output_path = filtered_citation_edges_root / f"filtered_citation_edges_{file_number}.parquet"
                if not (filtered_citation_edges_output_path.exists() and writers.is_valid_parquet(filtered_citation_edges_output_path)):
                    citation_edges_filter.filter_citation_edges(file_number, config, con)
            (citation_edges_tables_root / ".done").touch(exist_ok=True)
            logger.info("Completed all citation edges filtering")

        elif (filtered_citation_edges_root / ".done").exists():
            pass

        else:
            logger.error(f"Cannot filter citation edges because temp scores aren't finished")
            raise RuntimeError(f"Cannot filter citation edges because temp scores aren't finished")

        work_ids_buckets_count = 4

        if (filtered_citation_edges_root / ".done").exists() and (temp_scores_root / ".done").exists() and not (filtered_work_ids_root / ".done").exists():
            work_ids_filter.filter_work_ids(work_ids_buckets_count, config, con)
            (filtered_work_ids_root / ".done").touch(exist_ok=True)
            logger.info("Completed all work ids filtering")

            if (filtered_work_ids_root / ".done").exists():
                shutil.rmtree(temp_scores_root)

        elif (filtered_work_ids_root / ".done").exists() or ((filtered_works_root / ".done").exists() and (filtered_tables_root / "relationship_tables.done").exists()):
            pass

        else:
            if not ((filtered_citation_edges_root / ".done").exists() or (temp_scores_root / ".done").exists()):
                logger.error(f"Cannot filter work ids because filtered citation edges and temp scores aren't finished")
                raise RuntimeError(f"Cannot filter work ids because filtered citation edges and temp scores aren't finished")
            elif not (temp_scores_root / ".done").exists():
                logger.error(f"Cannot filter work ids because temp scores aren't finished")
                raise RuntimeError(f"Cannot filter work ids because temp scores aren't finished")
            elif not (filtered_citation_edges_root / ".done").exists():
                logger.error(f"Cannot filter work ids because filtered citation edges aren't finished")
                raise RuntimeError(f"Cannot filter work ids because filtered citation edges aren't finished")

        if (filtered_work_ids_root / ".done").exists():
            for file_number in range(works_file_count):
                filtered_works_output_path = filtered_works_root / f"filtered_works_{file_number}.parquet"
                if not (filtered_works_output_path.exists() and writers.is_valid_parquet(filtered_works_output_path)):
                    works_filter.filter_works_table(file_number, config, con)
            if not (filtered_works_root / ".done").exists():
                (filtered_works_root / ".done").touch(exist_ok=True)
                logger.info("Completed all works filtering")

            relationship_table_files_exist = []

            for table in RELATIONSHIP_TABLES:
                sub_tables_root = tables_root / table
                sub_filtered_tables_root = filtered_tables_root / f"filtered_{table}"

                table_file_count = len([file for file in sub_tables_root.glob("*.parquet")])

                for file_number in range(table_file_count):
                    if not (sub_filtered_tables_root / f"filtered_{table}_{file_number}.parquet").exists():
                        relationship_tables_filter.filter_relationship_table(table, file_number, config, con)

                    relationship_table_files_exist.append((sub_filtered_tables_root / f"filtered_{table}_{file_number}.parquet").exists())
            
            if relationship_table_files_exist and all(relationship_table_files_exist) and (filtered_works_root / ".done").exists():
                (filtered_tables_root / "relationship_tables.done").touch(exist_ok=True)
                shutil.rmtree(filtered_work_ids_root)
                logger.info("Completed all relationship tables filtering")

        elif ((filtered_works_root / ".done").exists() and (filtered_tables_root / "relationship_tables.done").exists()):
            pass
        
        else:
            logger.error(f"Cannot filter works and relationship tables because filtered work ids aren't finished")
            raise RuntimeError(f"Cannot filter works and relationship tables because filtered work ids aren't finished")

        reference_table_files_exist = []

        if (filtered_tables_root / "relationship_tables.done").exists():
            for name, values in REFERENCE_TABLES.items():
                sub_tables_root = tables_root / name
                sub_filtered_tables_root = filtered_tables_root / f"filtered_{name}"

                table_file_count = len([file for file in sub_tables_root.glob("*.parquet")])

                for file_number in range(table_file_count):
                    if not (sub_filtered_tables_root / f"filtered_{name}_{file_number}.parquet").exists():
                        reference_tables_filter.filter_reference_table(file_number, name, values, config, con)
                    
                    reference_table_files_exist.append((sub_filtered_tables_root / f"filtered_{name}_{file_number}.parquet").exists())

            if reference_table_files_exist and all(reference_table_files_exist):
                (filtered_tables_root / "reference_tables.done").touch(exist_ok=True)
                logger.info("Completed all reference tables filtering")
        
        elif ((filtered_tables_root / "reference_tables.done").exists()):
            pass

        else:
            logger.error(f"Cannot filter reference tables because filtered relationship tables aren't finished")
            raise RuntimeError(f"Cannot filter reference tables because filtered relationship tables aren't finished")

    finally:
        con.close()