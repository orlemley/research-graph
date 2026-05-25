from research_graph.processing import writers

from research_graph.pipelines import works_pipeline
from research_graph.pipelines import authors_pipeline
from research_graph.pipelines import deduplication_pipeline


PIPELINES = {
    "works": works_pipeline.run,
    "authors": authors_pipeline.run,
    "deduplication": deduplication_pipeline.run
}


def run_pipeline(context):
    config = context.config

    if config["temp_cleanup"]:
        writers.cleanup_temp_files(config["base_path"] / config["data_path"])

    enabled_pipelines = config["enabled_pipelines"]
    
    for pipeline_name in enabled_pipelines:
        PIPELINES[pipeline_name](context)