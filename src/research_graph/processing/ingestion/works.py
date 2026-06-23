import logging
import time
from research_graph.processing.ingestion import schemas
from research_graph.processing import paths
from research_graph.processing.ingestion import transforms
from research_graph.processing.io import shards
from research_graph.processing.io import writers


logger = logging.getLogger(__name__)


def process_works_shard(shard_key, context):
    config = context.config
    selected_concepts = config['selected_concepts']

    shard_name = shards.get_shard_name(shard_key)
    output_paths = paths.works_output_paths(shard_name, config["shards_root"])

    works_writers = {
    "works": writers.BatchedParquetWriter(output_paths["works"], (schemas.WORKS_SCHEMA).append(schemas.ABSTRACT_INVERTED_INDEX_FIELD) if config["include_abstract_inverted"] else schemas.WORKS_SCHEMA, compression = config["parquet_compression"]),
    "citation_edges": writers.BatchedParquetWriter(output_paths["citation_edges"], schemas.CITATION_EDGES_SCHEMA, compression = config["parquet_compression"]),
    "authorships": writers.BatchedParquetWriter(output_paths["authorships"], schemas.AUTHORSHIPS_SCHEMA, compression = config["parquet_compression"]),
    "affiliations": writers.BatchedParquetWriter(output_paths["affiliations"],schemas.AFFILIATIONS_SCHEMA, compression = config["parquet_compression"]),
    "institutions": writers.BatchedParquetWriter(output_paths["institutions"], schemas.INSTITUTIONS_SCHEMA, compression = config["parquet_compression"]),
    "sources": writers.BatchedParquetWriter(output_paths["sources"], schemas.SOURCES_SCHEMA, compression = config["parquet_compression"]),
    "concepts": writers.BatchedParquetWriter(output_paths["concepts"], schemas.CONCEPTS_SCHEMA, compression = config["parquet_compression"]),
    "scores": writers.BatchedParquetWriter(output_paths["scores"], schemas.SCORES_SCHEMA, compression = config["parquet_compression"])
    }

    if selected_concepts["enabled"]:
        works_writers["selected_scores"] =  writers.BatchedParquetWriter(output_paths["selected_scores"], schemas.SELECTED_SCORES_SCHEMA, compression = config["parquet_compression"])
        
    start_time = time.perf_counter()
    
    shard_papers_filtered = 0
    shard_papers_processed = 0
    shard_papers_failed = 0

    logger.info(f"{shard_name} started")

    for paper in shards.open_shard(shard_key, context):
        publication_year = paper.get('publication_year', -1)
        try:
            publication_year = int(publication_year)
        except (TypeError, ValueError):
            shard_papers_filtered += 1
            continue
        if publication_year is None or not (publication_year >= config["min_publication_year"] and publication_year <= config["max_publication_year"]):
            shard_papers_filtered += 1
            continue
        try:
            works_writers["works"].write(transforms.get_works_row(paper, config["include_abstract_inverted"]))
            for row in transforms.get_citation_edges_rows(paper):
                works_writers["citation_edges"].write(row)
            for row in transforms.get_authorships_rows(paper):
                works_writers["authorships"].write(row)
            for row in transforms.get_affiliations_rows(paper):
                works_writers["affiliations"].write(row)
            for row in transforms.get_institutions_rows(paper):
                works_writers["institutions"].write(row)
            works_writers["sources"].write(transforms.get_sources_row(paper))
            for row in transforms.get_concepts_rows(paper, config["concept_score_threshold"]):
                works_writers["concepts"].write(row)
            for row in transforms.get_scores_rows(paper, config["concept_score_threshold"]):
                works_writers["scores"].write(row)
            if selected_concepts["enabled"]:
                for row in transforms.get_selected_scores_rows(paper, selected_concepts):
                    works_writers["selected_scores"].write(row)
        except Exception as e:
            logger.exception(f"Failed paper {paper.get('id')}: {e}")
            shard_papers_failed += 1
            failure_path = config["shards_root"] / f"{shard_name}.works_failures.jsonl"
            try:
                writers.write_failure_record(
                    failure_path, 
                    {
                        "shard_key": shard_key,
                        "shard_name": shard_name,
                        "stage": "transform",
                        "work_id": paper.get("id", None),
                        "error_type": type(e).__name__,
                        "error": str(e),
                    }
                )
            except Exception:
                logger.exception(f"Failed to write paper failure record")
                raise RuntimeError
            continue

        shard_papers_processed += 1

    logger.info(f"{shard_name}: {shard_papers_processed} papers processed, {shard_papers_failed} papers failed")

    for writer in works_writers.values():
        writer.close()

    elapsed = time.perf_counter() - start_time

    papers_per_second = shard_papers_processed / elapsed if elapsed > 0 else 0

    logger.info(f"{shard_name} finished")

    return {
        "success": True,
        "papers_processed": shard_papers_processed,
        "papers_filtered": shard_papers_filtered,
        "papers_failed": shard_papers_failed,
        "time_to_complete": elapsed,
        "papers_per_second": papers_per_second
    }