import logging
from research_graph import config
from research_graph.processing import schemas
from research_graph.processing import paths
from research_graph.processing import transforms
from research_graph.processing import shards
from research_graph.processing import writers


logger = logging.getLogger(__name__)


def process_works_shard(shard_key):
    shard_name = shards.get_shard_name(shard_key)
    output_paths = paths.works_output_paths(shard_name)

    works_writers = {
    "works": writers.BatchedParquetWriter(output_paths["works"], schemas.WORKS_SCHEMA),
    "citation_edges": writers.BatchedParquetWriter(output_paths["citation_edges"], schemas.CITATION_EDGES_SCHEMA),
    "authorships": writers.BatchedParquetWriter(output_paths["authorships"], schemas.AUTHORSHIPS_SCHEMA),
    "affiliations": writers.BatchedParquetWriter(output_paths["affiliations"],schemas.AFFILIATIONS_SCHEMA),
    "institutions": writers.BatchedParquetWriter(output_paths["institutions"], schemas.INSTITUTIONS_SCHEMA),
    "venues": writers.BatchedParquetWriter(output_paths["venues"], schemas.VENUES_SCHEMA),
    "concepts": writers.BatchedParquetWriter(output_paths["concepts"], schemas.CONCEPTS_SCHEMA),
    "scores": writers.BatchedParquetWriter(output_paths["scores"], schemas.SCORES_SCHEMA)
    }

    shard_papers_filtered = 0
    shard_papers_processed = 0
    shard_papers_failed = 0

    logger.info(f"{shard_name} started")

    for paper in shards.open_shard(shard_key):
        publication_year = paper.get('publication_year', -1)
        try:
            publication_year = int(publication_year)
        except (TypeError, ValueError):
            shard_papers_filtered += 1
            continue
        if publication_year is None or not (publication_year >= config.MIN_PUBLICATION_YEAR and publication_year <= config.MAX_PUBLICATION_YEAR):
            shard_papers_filtered += 1
            continue
        try:
            works_writers["works"].write(transforms.get_works_row(paper))
            for row in transforms.get_citation_edges_rows(paper):
                works_writers["citation_edges"].write(row)
            for row in transforms.get_authorships_rows(paper):
                works_writers["authorships"].write(row)
            for row in transforms.get_affiliations_rows(paper):
                works_writers["affiliations"].write(row)
            for row in transforms.get_institutions_rows(paper):
                works_writers["institutions"].write(row)
            works_writers["venues"].write(transforms.get_venues_row(paper))
            for row in transforms.get_concepts_rows(paper):
                works_writers["concepts"].write(row)
            for row in transforms.get_scores_rows(paper):
                works_writers["scores"].write(row)
        except Exception as e:
            logger.warning(f"Failed paper {paper.get('id')}: {e}")
            shard_papers_failed += 1
            continue

        shard_papers_processed += 1

    logger.info(f"{shard_name}: {shard_papers_processed} papers processed, {shard_papers_failed} papers failed")

    for writer in works_writers.values():
        writer.close()

    logger.info(f"{shard_name} finished")

    return {
        "success": True,
        "papers_processed": shard_papers_processed,
        "papers_filtered": shard_papers_filtered,
        "papers_failed": shard_papers_failed
    }