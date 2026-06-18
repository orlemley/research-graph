import logging
import duckdb
from research_graph.processing.metrics import works_metrics


logger = logging.getLogger(__name__)


def run(context):
    config = context.config

    metrics_root = config["metrics_root"]

    metrics_root.mkdir(parents=True, exist_ok=True)

    works_metrics_root = metrics_root / "works_metrics"

    works_metrics_path_1 = works_metrics_root / "works_metrics_stage_1.parquet"
    works_metrics_path_2 = works_metrics_root / "works_metrics_stage_2.parquet"
    works_metrics_path_final = works_metrics_root / "works_metrics_final_stage.parquet"

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
        elif not works_metrics_path_1.exists() and not works_metrics_path_2.exists():
            logger.error("Cannot process works metrics stage 2 because stage 1 doesn't exist yet")
            raise RuntimeError("Cannot process works metrics stage 2 because stage 1 doesn't exist yet")
        
        if works_metrics_path_2.exists() and not works_metrics_path_final.exists():
            logger.info("Starting works metrics final stage")
            works_metrics.create_works_metrics_final_stage(config, con)
            logger.info("Finished works metrics final stage")
        elif not works_metrics_path_2.exists() and not works_metrics_path_final.exists():
            logger.error("Cannot process works metrics final stage because stage 2 doesn't exist yet")
            raise RuntimeError("Cannot process works metrics final stage because stage 2 doesn't exist yet")