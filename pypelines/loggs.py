import logging, sys
from functools import wraps
import coloredlogs

NAMELENGTH = 33
LEVELLENGTH = 8


def enable_logging(terminal_level="INFO", file_level="LOAD", programname="", username=""):
    # Create a filehandler object for file
    fh = logging.FileHandler("test.log")
    f_formater = FileFormatter()
    fh.setFormatter(f_formater)

    coloredlogs.HostNameFilter.install(
        fmt=f_formater.FORMAT,
        handler=fh,
        style=f_formater.STYLE,
        use_chroot=True,
    )
    coloredlogs.ProgramNameFilter.install(
        fmt=f_formater.FORMAT,
        handler=fh,
        programname=programname,
        style=f_formater.STYLE,
    )
    coloredlogs.UserNameFilter.install(
        fmt=f_formater.FORMAT,
        handler=fh,
        username=username,
        style=f_formater.STYLE,
    )

    # Create a filehandler object for terminal
    ch = logging.StreamHandler(sys.stdout)
    c_formater = TerminalFormatter()
    ch.setFormatter(c_formater)

    coloredlogs.HostNameFilter.install(
        fmt=c_formater.FORMAT,
        handler=fh,
        style=c_formater.STYLE,
        use_chroot=True,
    )
    coloredlogs.ProgramNameFilter.install(
        fmt=c_formater.FORMAT,
        handler=fh,
        programname=programname,
        style=c_formater.STYLE,
    )
    coloredlogs.UserNameFilter.install(
        fmt=c_formater.FORMAT,
        handler=fh,
        username=username,
        style=c_formater.STYLE,
    )

    logger = logging.getLogger()  # root logger

    while logger.hasHandlers():
        # make sure we start fresh from any previous handlers when we enable
        handler = logger.handlers[0]
        logger.removeHandler(handler)

    add_all_custom_headers()

    file_level = getattr(logging, file_level.upper())
    terminal_level = getattr(logging, terminal_level.upper())

    logger = logging.getLogger()

    logger.setLevel(
        min(terminal_level, file_level)
    )  # set logger level to the lowest usefull, to be sure we can capture messages necessary in handlers

    fh.setLevel(file_level)
    logger.addHandler(fh)
    ch.setLevel(terminal_level)
    logger.addHandler(ch)


class DynamicColoredFormatter(coloredlogs.ColoredFormatter):
    # note that only message, name, levelname, pathname, process, thread, lineno, levelno and filename can be dynamic.
    # asctime of hostname for example, can't. This is limitation for implementation simplicity reasons only,
    # as it would be more complex to implement otherwise, and for a small benefit.

    def __init__(self, fmt=None, datefmt=None, style="%", level_styles=None, field_styles=None, dynamic_levels=None):
        self.dynamic_levels = dynamic_levels
        self.lenght_pre_formaters = self.get_length_pre_formaters(fmt)
        super().__init__(
            fmt=fmt,
            datefmt=datefmt,
            style=style,
            level_styles=level_styles,
            field_styles=field_styles,
        )

    def get_length_pre_formaters(self, fmt):
        import re

        unallowed_part_names = [""]

        pattern = r"%\((?P<part_name>\w+)\)-?(?P<length>\d+)?[sd]?"
        result = re.findall(pattern, fmt)
        padding_dict = {name: int(padding) if padding else 0 for name, padding in result}

        return padding_dict

    def format(self, record):
        style = self.nn.get(self.level_styles, record.levelname)
        # print(repr(humanfriendly.terminal.ansi_style(**style)))
        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        if style and coloredlogs.Empty is not None:
            copy = coloredlogs.Empty()
            copy.__class__ = record.__class__
            copy.__dict__.update(record.__dict__)
            for part_name, length in self.lenght_pre_formaters.items():
                part = getattr(copy, part_name).ljust(length, " ")
                if part_name in self.dynamic_levels.keys():
                    dyn_keys = self.dynamic_levels[part_name]
                    dynamic_style = {k: v for k, v in style.items() if k in dyn_keys or dyn_keys == "all"}
                    part = coloredlogs.ansi_wrap(coloredlogs.coerce_string(part), **dynamic_style)
                setattr(copy, part_name, part)
            record = copy

        s = self.formatMessage(record)
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + record.exc_text
        if record.stack_info:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + self.formatStack(record.stack_info)
        return s


class SugarColoredFormatter(DynamicColoredFormatter):
    STYLE = "%"
    FORMAT = "%(levelname)-12s : %(name)-12s : %(message)s - %(asctime)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    LEVEL_STYLES = {
        "critical": {"bold": True, "color": 124},
        "error": {"color": 9},
        "warning": {"color": 214},
        "open": {
            "color": 57,
            "background": 195,
        },
        "close": {"color": 57, "background": 195},
        "header": {"color": 27, "underline": True, "bold": True},
        "load": {"color": 141, "italic": True},
        "save": {"color": 141, "italic": True},
        "info": {"color": 27},
        "debug": {"color": 8, "faint": True},
    }
    FIELD_STYLES = {
        "asctime": {"color": "green"},
        "hostname": {"color": "magenta"},
        "levelname": {"bold": True},
        "name": {"color": 19},
    }
    DYNAMIC_LEVELS = {
        "message": ["color", "underline"],
        "levelname": "all",
        "name": "all",
    }

    def __init__(self, fmt=None, datefmt=None, style=None, level_styles=None, field_styles=None, dynamic_levels=None):
        self.STYLE = style if style is not None else self.STYLE
        self.FORMAT = fmt if fmt is not None else self.FORMAT
        self.DATE_FORMAT = datefmt if datefmt is not None else self.DATE_FORMAT
        self.LEVEL_STYLES = level_styles if level_styles is not None else self.LEVEL_STYLES
        self.FIELD_STYLES = field_styles if field_styles is not None else self.FIELD_STYLES

        self.DYNAMIC_LEVELS = dynamic_levels if dynamic_levels is not None else self.DYNAMIC_LEVELS

        super().__init__(
            fmt=self.FORMAT,
            datefmt=self.DATE_FORMAT,
            style=self.STYLE,
            level_styles=self.LEVEL_STYLES,
            field_styles=self.FIELD_STYLES,
            dynamic_levels=self.DYNAMIC_LEVELS,
        )


class TerminalFormatter(SugarColoredFormatter):
    FORMAT = f"%(levelname)-{LEVELLENGTH}s : %(name)-{NAMELENGTH}s : %(message)s - %(asctime)s"


class FileFormatter(SugarColoredFormatter):
    FORMAT = f"[%(asctime)s] %(hostname)s %(levelname)-{LEVELLENGTH}s : %(name)-{NAMELENGTH}s : %(message)s"


class LogTask:
    def __init__(self, worker_pk, level="INFO"):
        self.worker_pk = worker_pk
        self.level = getattr(logging, level.upper())

    def __enter__(self):
        self.logger = logging.getLogger()
        self.set_handler()
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove_handler()

    def set_handler(self):
        fh = logging.FileHandler(f"{self.worker_pk}.log")
        fh.setLevel(self.level)
        fh.setFormatter(FileFormatter())
        self.logger.addHandler(fh)

    def remove_handler(self):
        self.logger.removeHandler(self.logger.handlers[-1])


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


def add_all_custom_headers():
    addLoggingLevel("LOAD", logging.DEBUG + 1, if_exists="keep")
    addLoggingLevel("SAVE", logging.DEBUG + 2, if_exists="keep")
    addLoggingLevel("HEADER", logging.INFO + 1, if_exists="keep")
    addLoggingLevel("OPEN", logging.INFO + 2, if_exists="keep")
    addLoggingLevel("CLOSE", logging.INFO + 3, if_exists="keep")


def addLoggingLevel(levelName, levelNum, methodName=None, if_exists="raise"):
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `levelName` becomes an attribute of the `logging` module with the value
    `levelNum`. `methodName` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `methodName` is not specified, `levelName.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    Example
    -------
    >>> addLoggingLevel('TRACE', logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel("TRACE")
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
    5

    """
    if not methodName:
        methodName = levelName.lower()

    if hasattr(logging, levelName) or hasattr(logging, methodName) or hasattr(logging.getLoggerClass(), methodName):
        if if_exists == "keep":
            return
        if hasattr(logging, levelName):
            raise AttributeError("{} already defined in logging module".format(levelName))
        if hasattr(logging, methodName):
            raise AttributeError("{} already defined in logging module".format(methodName))
        if hasattr(logging.getLoggerClass(), methodName):
            raise AttributeError("{} already defined in logger class".format(methodName))

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(levelNum):
            self._log(levelNum, message, args, **kwargs)

    def logToRoot(message, *args, **kwargs):
        logging.log(levelNum, message, *args, **kwargs)

    logging.addLevelName(levelNum, levelName)
    setattr(logging, levelName, levelNum)
    setattr(logging.getLoggerClass(), methodName, logForLevel)
    setattr(logging, methodName, logToRoot)
