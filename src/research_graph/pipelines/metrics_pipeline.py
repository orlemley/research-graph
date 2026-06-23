import logging
import duckdb
from research_graph.processing.metrics import works_metrics
from research_graph.processing.metrics import authors_metrics
from research_graph.processing.metrics import sources_metrics
from research_graph.processing.metrics import institutions_metrics


logger = logging.getLogger(__name__)


def run(context):
    config = context.config

    metrics_root = config["metrics_root"]

    metrics_root.mkdir(parents=True, exist_ok=True)

    works_metrics_root = metrics_root / "works_metrics"
    authors_metrics_root = metrics_root / "authors_metrics"
    sources_metrics_root = metrics_root / "sources_metrics"
    institutions_metrics_root = metrics_root / "institutions_metrics"

    works_metrics_path_1 = works_metrics_root / "works_metrics_stage_1.parquet"
    works_metrics_path_2 = works_metrics_root / "works_metrics_stage_2.parquet"
    works_metrics_path_final = works_metrics_root / "works_metrics.parquet"

    authors_metrics_path_1 = authors_metrics_root / "authors_metrics_stage_1.parquet"
    authors_metrics_path_2 = authors_metrics_root / "authors_metrics_stage_2.parquet"
    authors_metrics_path_final = authors_metrics_root / "authors_metrics.parquet"

    sources_metrics_path_1 = sources_metrics_root / "sources_metrics_stage_1.parquet"
    sources_metrics_path_2 = sources_metrics_root / "sources_metrics.parquet"

    institutions_metrics_path_1 = institutions_metrics_root / "institutions_metrics_stage_1.parquet"
    institutions_metrics_path_2 = institutions_metrics_root / "institutions_metrics.parquet"

    concepts = config["graph_filter_concepts"]

    with duckdb.connect() as con:

        if not works_metrics_path_1.exists() and not (works_metrics_path_2.exists() or works_metrics_path_final.exists()):
            logger.info("Starting works metrics stage 1")
            works_metrics.create_works_metrics_stage_1(concepts, config, con)
            logger.info("Finished works metrics stage 1")

        if works_metrics_path_1.exists() and not works_metrics_path_2.exists() and not works_metrics_path_final.exists():
            logger.info("Starting works metrics stage 2")
            works_metrics.create_works_metrics_stage_2(config, con)
            logger.info("Finished works metrics stage 2")
        elif not works_metrics_path_1.exists() and not works_metrics_path_2.exists() and not works_metrics_path_final.exists():
            logger.error("Cannot process works metrics stage 2 because stage 1 doesn't exist yet")
            raise RuntimeError("Cannot process works metrics stage 2 because stage 1 doesn't exist yet")
        
        if works_metrics_path_2.exists() and not works_metrics_path_final.exists():
            logger.info("Starting works metrics final stage")
            works_metrics.create_works_metrics_final_stage(config, con)
            logger.info("Finished works metrics final stage")
        elif not works_metrics_path_2.exists() and not works_metrics_path_final.exists():
            logger.error("Cannot process works metrics final stage because stage 2 doesn't exist yet")
            raise RuntimeError("Cannot process works metrics final stage because stage 2 doesn't exist yet")
        
        if not authors_metrics_path_1.exists() and not (authors_metrics_path_2.exists() or authors_metrics_path_final.exists()):
            logger.info("Starting authors metrics stage 1")
            authors_metrics.create_authors_metrics_stage_1(config, con)
            logger.info("Finished authors metrics stage 1")

        if authors_metrics_path_1.exists() and not authors_metrics_path_2.exists() and not authors_metrics_path_final.exists():
            logger.info("Starting authors metrics stage 2")
            authors_metrics.create_authors_metrics_stage_2(config, con)
            logger.info("Finished authors metrics stage 2")
        elif not authors_metrics_path_1.exists() and not authors_metrics_path_2.exists() and not authors_metrics_path_final.exists():
            logger.error("Cannot process authors metrics stage 2 because stage 1 doesn't exist yet")
            raise RuntimeError("Cannot process authors metrics stage 2 because stage 1 doesn't exist yet")
        
        if authors_metrics_path_2.exists() and not authors_metrics_path_final.exists():
            logger.info("Starting authors metrics final stage")
            authors_metrics.create_authors_metrics_final_stage(config, con)
            logger.info("Finished authors metrics final stage")
        elif not authors_metrics_path_2.exists() and not authors_metrics_path_final.exists():
            logger.error("Cannot process authors metrics final stage because stage 2 doesn't exist yet")
            raise RuntimeError("Cannot process authors metrics final stage because stage 2 doesn't exist yet")

        if not sources_metrics_path_1.exists() and not (sources_metrics_path_2.exists()):
            logger.info("Starting sources metrics stage 1")
            sources_metrics.create_sources_metrics_stage_1(config, con)
            logger.info("Finished sources metrics stage 1")

        if sources_metrics_path_1.exists() and not sources_metrics_path_2.exists():
            logger.info("Starting sources metrics stage 2")
            sources_metrics.create_sources_metrics_stage_2(config, con)
            logger.info("Finished sources metrics stage 2")
        elif not sources_metrics_path_1.exists() and not sources_metrics_path_2.exists():
            logger.error("Cannot process sources metrics stage 2 because stage 1 doesn't exist yet")
            raise RuntimeError("Cannot process sources metrics stage 2 because stage 1 doesn't exist yet")
        
        if not institutions_metrics_path_1.exists() and not (institutions_metrics_path_2.exists()):
            logger.info("Starting institutions metrics stage 1")
            institutions_metrics.create_institutions_metrics_stage_1(config, con)
            logger.info("Finished institutions metrics stage 1")
        
        if institutions_metrics_path_1.exists() and not institutions_metrics_path_2.exists():
            logger.info("Starting institutions metrics stage 2")
            institutions_metrics.create_institutions_metrics_stage_2(config, con)
            logger.info("Finished institutions metrics stage 2")
        elif not institutions_metrics_path_1.exists() and not institutions_metrics_path_2.exists():
            logger.error("Cannot process institutions metrics stage 2 because stage 1 doesn't exist yet")
            raise RuntimeError("Cannot process institutions metrics stage 2 because stage 1 doesn't exist yet")