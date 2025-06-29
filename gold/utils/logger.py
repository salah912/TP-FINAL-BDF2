import logging
import os

def get_logger(name, log_file):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(fh)

    return logger
