import logging


def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger with the name of the module that called this function.
    :return: A logger.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(name)
    return logger
