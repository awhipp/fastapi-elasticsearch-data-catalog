# Setup logging and log timestamp prepend
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s', 
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)


def get_logger(name):
    '''Returns a logger with default config'''
    logger = logging.getLogger(name=name)
    return logger