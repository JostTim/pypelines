import logging
from functools import wraps


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
