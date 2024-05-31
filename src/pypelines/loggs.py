import logging
import sys
import re
from functools import wraps
from coloredlogs import (
    coerce_string,
    ansi_wrap,
    Empty,
    ColoredFormatter,
    UserNameFilter,
    ProgramNameFilter,
    HostNameFilter,
)
from pathlib import Path

from typing import Protocol, Callable, cast

NAMELENGTH = 33  # global variable for formatting the length of the padding dedicated to name part in a logging record
LEVELLENGTH = 9  # global variable for formatting the length of the padding dedicated to levelname part in a record


class PypelineLoggerProtocol(Protocol):
    def save(self, msg, *args, **kwargs) -> None: ...
    def load(self, msg, *args, **kwargs) -> None: ...
    def note(self, msg, *args, **kwargs) -> None: ...
    def start(self, msg, *args, **kwargs) -> None: ...
    def end(self, msg, *args, **kwargs) -> None: ...
    def header(self, msg, *args, **kwargs) -> None: ...


class PypelineLogger(logging.Logger, PypelineLoggerProtocol):
    pass


getLogger = cast(Callable[[str], PypelineLogger], logging.getLogger)


def enable_logging(
    filename: str | None = None,
    terminal_level: str = "NOTE",
    file_level: str = "LOAD",
    programname: str = "",
    username: str = "",
):
    """Enable logging with specified configurations.

    Args:
        filename (str, optional): Path to the log file. Defaults to None.
        terminal_level (str, optional): Logging level for terminal output. Defaults to "INFO".
        file_level (str, optional): Logging level for file output. Defaults to "LOAD".
        programname (str, optional): Name of the program. Defaults to "".
        username (str, optional): Username for logging. Defaults to "".
    """
    # Create a filehandler object for file
    if filename is None:
        logs_folder = Path.home() / ".python" / "pypelines_logs"
        logs_folder.mkdir(parents=True, exist_ok=True)
        filename = str(logs_folder / "logs.log")

    fh = logging.FileHandler(filename, mode="a", encoding="utf-8")
    f_formater = FileFormatter()
    fh.setFormatter(f_formater)

    # Create a filehandler object for terminal
    ch = logging.StreamHandler(sys.stdout)
    c_formater = TerminalFormatter()
    ch.setFormatter(c_formater)

    for handler, formater in zip([fh, ch], [f_formater, c_formater]):

        HostNameFilter.install(
            fmt=formater.FORMAT,
            handler=handler,
            style=f_formater.STYLE,
            use_chroot=True,
        )
        ProgramNameFilter.install(
            fmt=formater.FORMAT,
            handler=handler,
            programname=programname,
            style=formater.STYLE,
        )
        UserNameFilter.install(
            fmt=formater.FORMAT,
            handler=handler,
            username=username,
            style=formater.STYLE,
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

    for handler, level in zip([fh, ch], [file_level, terminal_level]):

        handler.setLevel(level)
        logger.addHandler(handler)


class DynamicColoredFormatter(ColoredFormatter):
    """_summary_"""

    # note that only message, name, levelname, pathname, process, thread, lineno, levelno and filename can be dynamic.
    # asctime of hostname for example, can't. This is limitation for implementation simplicity reasons only,
    # as it would be more complex to implement otherwise, and for a small benefit.

    def __init__(self, fmt=None, datefmt=None, style="%", level_styles=None, field_styles=None, dynamic_levels=None):
        """Initialize the logging formatter with custom formatting options.

        Args:
            fmt (str, optional): A format string for the log message. Defaults to None.
            datefmt (str, optional): A format string for the date/time portion of the log message. Defaults to None.
            style (str, optional): The style of formatting to use. Defaults to "%".
            level_styles (dict, optional): A dictionary mapping log levels to custom styles. Defaults to None.
            field_styles (dict, optional): A dictionary mapping log fields to custom styles. Defaults to None.
            dynamic_levels (dict, optional): A dictionary mapping dynamic log levels. Defaults to None.
        """
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
        """Get the length of pre-formatters in the given format string.

        Args:
            fmt (str): The format string containing pre-formatters.

        Returns:
            dict: A dictionary containing the length of each pre-formatter.
        """
        pattern = r"%\((?P<part_name>\w+)\)-?(?P<length>\d+)?[sd]?"
        result = re.findall(pattern, fmt)
        padding_dict = {name: int(padding) if padding else 0 for name, padding in result}

        return padding_dict

    def format(self, record: logging.LogRecord):
        """Format the log record for display.

        Args:
            record (logging.LogRecord): The log record to be formatted.

        Returns:
            str: The formatted log message.
        """
        style = self.nn.get(self.level_styles, record.levelname)
        # print(repr(humanfriendly.terminal.ansi_style(**style)))
        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        if style and Empty is not None:
            copy = Empty()
            copy.__class__ = record.__class__
            copy.__dict__.update(record.__dict__)
            for part_name, length in self.lenght_pre_formaters.items():
                part = getattr(copy, part_name)  # .ljust(length, " ")
                real_length = len(part)
                missing_length = length - real_length
                missing_length = 0 if missing_length < 0 else missing_length
                if part_name in self.dynamic_levels.keys():
                    dyn_keys = self.dynamic_levels[part_name]
                    dynamic_style = {k: v for k, v in style.items() if k in dyn_keys or dyn_keys == "all"}
                    part = ansi_wrap(coerce_string(part), **dynamic_style)
                part = part + (" " * missing_length)
                setattr(copy, part_name, part)
            record = copy  # type: ignore

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
        "start": {"color": 195, "background": 57, "bold": True},
        "end": {"color": 195, "background": 57, "bold": True},
        "header": {"color": 27, "underline": True, "bold": True},
        "info": {"color": 27},
        "note": {"color": 8},
        "load": {"color": 141, "italic": True},
        "save": {"color": 141, "italic": True},
        "debug": {"color": 251, "faint": True},
    }
    FIELD_STYLES = {
        "asctime": {"color": "green"},
        "hostname": {"color": "magenta"},
        "levelname": {"bold": True},
        "name": {"color": 19},
    }
    DYNAMIC_LEVELS = {
        "message": ["color", "bold", "background"],
        "levelname": "all",
        "name": "all",
    }

    def __init__(self, fmt=None, datefmt=None, style=None, level_styles=None, field_styles=None, dynamic_levels=None):
        """Initializes a custom logging formatter with specified parameters.

        Args:
            fmt (str): The log message format string.
            datefmt (str): The date format string.
            style (str): The log message style.
            level_styles (dict): Dictionary mapping log levels to custom styles.
            field_styles (dict): Dictionary mapping log fields to custom styles.
            dynamic_levels (bool): Flag indicating whether dynamic levels are enabled.

        Returns:
            None
        """
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
    FORMAT = f"%(levelname)-{LEVELLENGTH}s: %(name)-{NAMELENGTH}s : %(message)s - %(asctime)s"


class FileFormatter(SugarColoredFormatter):
    FORMAT = f"[%(asctime)s] %(hostname)s %(levelname)-{LEVELLENGTH}s: %(name)-{NAMELENGTH}s : %(message)s"


class ContextFilter(logging.Filter):
    """This is a filter which injects contextual information into the log."""

    def __init__(self, context_msg):
        """Initialization method.

        Args:
            context_msg (str): Context to inject into the log
        """
        self.context_msg = context_msg

    def filter(self, record):
        """Modify log record to include the context message.

        Args:
            record (logging.LogRecord): Log record.

        Returns:
            bool: Always True since the filter does not block any record
        """
        record.msg = f"{self.context_msg} {record.msg}"
        return True

    def __repr__(self):
        """String representation of the ContextFilter.

        Returns:
            str: String representation of ContextFilter.
        """
        return f"<ContextFilter({self.context_msg})>"


class LogContext:
    """A context for managing logging with context_msg added to any logging entry inside it"""

    def __init__(self, context_msg):
        """Initialization method.

        Args:
            context_msg (str): Context message to log.

        Example :
            ```python
            logger = logging.getLogger("test_level")
            with LogContext("my context"):
                logger.info("a test message in a context")
            logger.info("a test message out of a context")
            ```

            ```console
            INFO     : test_level                    : <my context> a test message in a context
            INFO     : test_level                    : a test message out of a context
            ```
        """
        self.context_msg = context_msg
        self.context_filters = {}

    def __enter__(self):
        """Add context specific filter to a logging handler"""
        self.root_logger = logging.getLogger()
        found = False
        for handler in self.root_logger.handlers:
            for filter in handler.filters:
                if getattr(filter, "context_msg", "") == self.context_msg:
                    self.root_logger.debug(f"Filter already added to handler {handler}")
                    found = True
                    break

        if found:
            return

        # else add it to any handler coming first
        for handler in self.root_logger.handlers:
            context_filter = ContextFilter(self.context_msg)
            handler.addFilter(context_filter)
            self.context_filters[handler] = context_filter
            self.root_logger.debug(f"Added filter {context_filter} to handler {handler}")
            break

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Remove previously added context-specific filter from a logging handler.

        Args:
            exc_type (type): Type of the exception that caused the context management to be exited.
            exc_val (Exception): Instance of the exception.
            exc_tb (traceback): Traceback of the exception.
        """
        for handler in self.root_logger.handlers:
            filer_to_remove = self.context_filters.get(handler, None)
            if filer_to_remove is None:
                continue
            else:
                self.root_logger.debug(f"Removing filter {filer_to_remove} from handler {handler} in this context")
                handler.removeFilter(filer_to_remove)


class LogSession(LogContext):
    """Specialized version of LogContext for managing session logging."""

    def __init__(self, session):
        """Initialization method.

        Args:
            session (dict): Session details.
        """
        context_msg = "<" + str(session["alias"]) + ">"
        super().__init__(context_msg)


def loggedmethod(func):
    """Decorator to perform logged method.

    Args:
        func (function): Function to be executed with logging.

    Returns:
        function: Decorated function.
    """

    @wraps(func)
    def wrapper(session, *args, **kwargs):
        if kwargs.get("no_session_log", False):
            return func(session, *args, **kwargs)
        with LogSession(session):
            return func(session, *args, **kwargs)

    return wrapper


def add_all_custom_headers():
    """Adds custom logging levels to the logging module.

    This function adds custom logging levels "NOTE", "LOAD", "SAVE", "HEADER", "START",
    and "END" to the logging module with specific integer values relative to existing levels.

    Example:
        add_all_custom_headers()

    Note:
        This function should be called before using the custom logging levels in the application.
    """
    addLoggingLevel("NOTE", logging.INFO - 1, if_exists="keep")
    addLoggingLevel("LOAD", logging.DEBUG + 1, if_exists="keep")
    addLoggingLevel("SAVE", logging.DEBUG + 2, if_exists="keep")
    addLoggingLevel("HEADER", logging.INFO + 1, if_exists="keep")
    addLoggingLevel("START", logging.INFO + 2, if_exists="keep")
    addLoggingLevel("END", logging.INFO + 3, if_exists="keep")


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
