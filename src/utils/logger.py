import os
import logging

def setup_logger(name: str, log_file: str = "pipeline.log"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Always resolve log_file relative to project root if not absolute
    if not os.path.isabs(log_file):
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
        log_file_path = os.path.join(project_root, log_file)
    else:
        log_file_path = log_file

    log_directory = os.path.dirname(log_file_path) or "logs"
    os.makedirs(log_directory, exist_ok=True)

    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger