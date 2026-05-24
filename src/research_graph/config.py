import logging
from pathlib import Path
import uuid
from datetime import datetime
import yaml


def load_config():
    config_path = Path(__file__).parents[0] / "config.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    config["base_path"] = Path(__file__).resolve().parents[2]
    config["data_path"] = Path(config["data_path"])
    config["logs_path"] = Path(config["logs_path"])
    config["data_root"] = config["base_path"] / config["data_path"]
    return config


def setup_logging(base_path, logs_path):
    if logging.getLogger().handlers:
        return
    
    log_dir = base_path / logs_path
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_id = uuid.uuid4().hex[:6]

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(log_dir / f"pipeline_{timestamp}_{run_id}.log"),
            logging.StreamHandler()
        ]
    )