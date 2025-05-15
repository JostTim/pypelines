__version__ = "0.0.84"

from . import loggs
from .pipes import *
from .pipelines import *
from .steps import *
from .disk import *
from .sessions import *

from .extend_pandas import extend_pandas

# NOTE:
# pypelines is enabling the logging system by default when importing it
# (it comprises colored logging, session prefix-logging, and logging to a file located in downloads folder)
loggs.enable_logging()
