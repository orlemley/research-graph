from research_graph.context import Context
from research_graph.clients import create_s3_client
from research_graph.config import load_config, setup_logging
from research_graph.orchestration.runner import run_pipeline


def main():
    config = load_config()
    s3 = create_s3_client(config["aws_region"])

    context = Context(config=config, s3=s3)

    setup_logging(config["base_path"], config["logs_path"])

    run_pipeline(context)


if __name__ == "__main__":
    main()