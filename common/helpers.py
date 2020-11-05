'''
Some common helpers
'''
import logging


def setup_logging(name, level, path):
    '''
    logging setup with the requested level.
    '''
    log = logging.getLogger(name)
    log.setLevel(getattr(logging, level, logging.INFO))
    f_handler = logging.FileHandler(path)
    f_handler.setLevel(getattr(logging, level, logging.INFO))
    log_format = '%(asctime)s|%(levelname)s|%(message)s'
    f_handler.setFormatter(logging.Formatter(log_format))
    log.addHandler(f_handler)
    log.debug('Done setting up logging')
    return log
