"""Configures structured logging for the pipeline."""
import logging
import os


def setup_logging(log_dir="output"):
    """Set up logging to console and file with timestamps."""
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "pipeline.log")

    formatter = logging.Formatter(
        "%(asctime)s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_handler = logging.FileHandler(log_path, mode="w")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(file_handler)
    root.addHandler(console_handler)
