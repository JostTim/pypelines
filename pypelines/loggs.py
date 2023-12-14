import logging
from functools import wraps
import coloredlogs


def enable_logging(level="DEBUG", file_level="INFO"):
    import logging, os, sys

    # Create a logger object.
    logger = logging.getLogger()  # root logger

    # make sure we start fresh
    for handler in logger.handlers:
        logger.removeHandler(handler)

    # Create a filehandler object for file
    fh = logging.FileHandler("test.log")
    fh.setLevel(file_level)
    fh.setFormatter(FileFormatter())

    # Create a filehandler object for terminal
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(TerminalFormatter())

    logger.addHandler(fh)
    logger.addHandler(ch)


NAMELENGTH = 33
LEVELLENGTH = 10


class TerminalFormatter(coloredlogs.ColoredFormatter):
    FORMAT = f"%(levelname)-{LEVELLENGTH}s : %(name)-{NAMELENGTH}s : %(message)s  -  %(asctime)s"

    def __init__(self, fmt=FORMAT, datefmt=None, style="%"):
        super().__init__(
            fmt=fmt,
            datefmt=datefmt,
            style=style,
            level_styles={
                "critical": {"bold": True, "color": "red"},
                "debug": {},
                "error": {"color": "red"},
                "info": {"color": "blue"},
                "warning": {"color": "yellow"},
            },
            field_styles={
                "asctime": {"color": "green"},
                "hostname": {"color": "magenta"},
                #'levelname': {'bold': True, 'color': 'black'},
                "name": {"color": "blue"},
            },
        )


class FileFormatter(coloredlogs.ColoredFormatter):
    FORMAT = f"[%(asctime)s] - %(levelname)-{LEVELLENGTH}s : %(name)-{NAMELENGTH}s : %(message)s"

    def __init__(self, fmt=FORMAT, datefmt=None, style="%"):
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)


def set_handler_(logger, worker_pk, level="INFO"):
    fh = logging.FileHandler(f"{worker_pk}.log")
    fh.setLevel(level)
    fh.setFormatter(FileFormatter())
    logger.addHandler(fh)


def remove_handler(logger):
    logger.removeHandler(logger.handlers[-1])


class LogToLocalFile:
    def __init__(self, logger_name, worker_pk):
        self.logger_name = logger_name
        self.worker_pk = worker_pk

    def __enter__(self):
        self.logger = logging.getLogger(self.logger_name)
        set_logger_formatter(self.logger, self.worker_pk)
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        remove_logger_handler(self.logger)


class ContextFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.
    """

    def __init__(self, context_msg):
        self.context_msg = context_msg

    def filter(self, record):
        record.msg = f"{self.context_msg} {record.msg}"
        return True


class LogContext:
    def __init__(self, context_msg):
        self.context_msg = context_msg
        self.filter_was_added = False

    def __enter__(self):
        self.root_logger = logging.getLogger()
        for filter in self.root_logger.filters:
            if getattr(filter, "context_msg", "") == self.context_msg:
                return

        self.filter_was_added = True
        self.context_filter = ContextFilter(self.context_msg)
        self.root_logger.addFilter(self.context_filter)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.filter_was_added:
            self.root_logger.removeFilter(self.context_filter)


class LogSession(LogContext):
    def __init__(self, session):
        context_msg = "s#" + str(session.alias)
        super().__init__(context_msg)


def loggedmethod(func):
    @wraps(func)
    def wrapper(session, *args, **kwargs):
        if kwargs.get("no_session_log", False):
            return func(session, *args, **kwargs)
        with LogSession(session):
            return func(session, *args, **kwargs)

    return wrapper
