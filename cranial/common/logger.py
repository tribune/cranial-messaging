import logging.handlers
import os


class FileHandler(logging.handlers.RotatingFileHandler):
    def __init__(self, path='/home/', filename='cranial.log'):
        if path[-1] != '/':
            path += '/'

        logging.handlers.RotatingFileHandler.__init__(self,
                                                      path + filename,
                                                      maxBytes=5242880,
                                                      backupCount=10)
        fmt = '%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s'
        fmt_date = '%Y-%m-%dT%T%Z'
        formatter = logging.Formatter(fmt, fmt_date)

        self.setFormatter(formatter)


class StreamingHandler(logging.StreamHandler):
    def __init__(self):
        logging.StreamHandler.__init__(self)
        fmt = '%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s'
        fmt_date = '%Y-%m-%dT%T%Z'
        formatter = logging.Formatter(fmt, fmt_date)

        self.setFormatter(formatter)


def create(name, level, log_type='streaming', filename=None):
    """Create new logger

    :param name: name for logger
    :type name: str
    :param level:  logging level, e.g. logging.WARN
    :type level: int
    :param type
    :returns: logging object
    """
    log = logging.getLogger(name)
    log.setLevel(level)
    if not log.handlers:
        if log_type == 'streaming':
            log.addHandler(StreamingHandler())
        else:
            if filename is not None:
                log.addHandler(FileHandler(filename))
            else:
                log.addHandler(FileHandler())
    return log


def get(var='LOGLEVEL', level=None, name='cranial'):
    """ This is the recommended way to instantiate a logger if you
    don't care about the details.

    >>> log = get()
    >>> log.debug('spam')
    <BLANKLINE>
    >>> log.warning('Ni!') # doctest: +ELLIPSIS
    ... WARNING - Ni!
    """
    level = level or fallback(var)
    return create(name, level)


def fallback(var_name, fallback='WARN'):
    return os.environ.get(var_name,
                          os.environ.get('CRANIAL_LOGLEVEL', fallback))
