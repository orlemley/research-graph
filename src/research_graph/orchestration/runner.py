from research_graph.processing import writers

from research_graph.pipelines import works_pipeline
from research_graph.pipelines import authors_pipeline


def run_pipeline(context):
    config = context.config
    writers.cleanup_temp_files(config["base_path"] / config["data_path"])

    works_pipeline.run(context)
    authors_pipeline.run(context)