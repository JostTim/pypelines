from functools import wraps, partial, update_wrapper
from .loggs import loggedmethod
import logging
from typing import Callable

from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .pipeline import BasePipeline
    from .pipe import BasePipe
    from .disk import BaseDiskObject


def stepmethod(requires=[], version=None):
    # This method allows to register class methods inheriting of BasePipe as steps.
    # It basically just step an "is_step" stamp on the method that are defined as steps.
    # This stamp will later be used in the metaclass __new__ to set additionnal usefull attributes to those methods
    def registrate(function):
        function.requires = [requires] if not isinstance(requires, list) else requires
        function.is_step = True
        function.use_version = False if version is None else True
        function.version = version
        return function

    return registrate


class BaseStep:
    def __init__(
        self,
        pipeline: "BasePipeline",
        pipe: "BasePipe",
        step: "BaseStep",
        step_name: str,
    ):
        self.pipeline = pipeline  # save an instanciated access to the pipeline parent
        self.pipe = pipe  # save an instanciated access to the pipe parent
        self.step = (
            step  # save an instanciated access to the step function (undecorated)
        )
        self.pipe_name = pipe.pipe_name
        self.step_name = step_name
        self.full_name = f"{self.pipe_name}.{self.step_name}"
        self.use_version = self.step.use_version
        self.version = self.step.version

        self.single_step = self.pipe.single_step
        self.requires = self.step.requires
        self.is_step = True

        self.requirement_stack = partial(
            self.pipeline.get_requirement_stack, instance=self
        )
        # self.step_version = partial( self.pipe.step_version, step = self )

        update_wrapper(self, self.step)
        self._make_wrapped_functions()

    def __call__(self, *args, **kwargs):
        return self.step(*args, **kwargs)

    def __repr__(self):
        return f"<{self.pipe_name}.{self.step_name} StepObject>"

    def _make_wrapped_functions(self):
        self.make_wrapped_save()
        self.make_wrapped_load()
        self.make_wrapped_generate()

    def make_wrapped_save(self):
        def wrapper(session, data, extra=""):
            disk_object = self.pipe.disk_class(session, self, extra=extra)
            disk_object.check_disk()
            return disk_object.save(data)

        self.save = self.pipe.dispatcher(wrapper)

    def make_wrapped_load(self):
        def wrapper(session, extra=""):
            disk_object = self.pipe.disk_class(session, self, extra=extra)
            disk_object.check_disk()
            return disk_object.load()

        self.load = self.pipe.dispatcher(wrapper)

    def make_wrapped_generate(self):
        def wrapper(session, *args, extra="", **kwargs):
            disk_object = self.pipe.disk_class(session, self, extra=extra)
            return loggedmethod(
                self._load_or_generate_wrapper(
                    self._save_after_generate_wrapper(
                        self.pipe.pre_run_wrapper(self.step), disk_object
                    ),
                    disk_object,
                )
            )(session, *args, extra = extra, **kwargs)

        self.generate = self.pipe.dispatcher(wrapper)

    def step_current_version(self) -> str:
        # simply returns the current string of the version that is in the config file.
        return "version"
        ...

    def _version_wrapper(self, function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            version = self.step_current_version(self)
            return function(*args, version=version, **kwargs)

        return wrapper

    def _load_or_generate_wrapper(
        self, function: Callable, disk_object: "BaseDiskObject"
    ):
        """
        Decorator to load instead of calculating if not refreshing and saved data exists
        """

        @wraps(function)
        def wrap(session_details, *args, **kwargs):
            """
            Decorator function

            Parameters
            ----------
            *args : TYPE
                DESCRIPTION.
            **kwargs : TYPE
                DESCRIPTION.

            Returns
            -------
            TYPE
                DESCRIPTION.

            """
            logger = logging.getLogger("load_pipeline")

            kwargs = kwargs.copy()
            extra = kwargs.get("extra", "")
            # version = kwargs.get("version", "")
            skipping = kwargs.pop("skip", False)
            # we raise if file not found only if skipping is True
            refresh = kwargs.get("refresh", False)
            refresh_main_only = kwargs.get("refresh_main_only", False)

            if refresh_main_only:
                # we set refresh true no matter what and then set
                # refresh_main_only to False so that possible childs functions will never do this again
                refresh = True
                kwargs["refresh"] = False
                kwargs["refresh_main_only"] = False

            if refresh and skipping:
                raise ValueError(
                    """You tried to set refresh (or refresh_main_only) to True and skipping to True simultaneouly. 
                    Stopped code to prevent mistakes : You probably set this by error as both have antagonistic effects. 
                    (skipping passes without loading if file exists, refresh overwrites after generating output if file exists) 
                    Please change arguments according to your clarified intention."""
                )

            if not refresh:
                if disk_object.check_disk() and skipping:
                    logger.info(
                        f"File exists for {self.pipe_name}{'.' + extra if extra else ''}. Loading and processing have been skipped"
                    )
                    return None
                logger.debug(f"Trying to load saved data")
                try:
                    result = (
                        disk_object.load()
                    )  # self.pipe.file_loader(session_details, extra=extra, version=version)
                    logger.info(
                        f"Found and loaded {self.pipe_name}{'.' + extra if extra else ''} file. Processing has been skipped "
                    )
                    return result
                except IOError:
                    logger.info(
                        f"Could not find or load {self.pipe_name}{'.' + extra if extra else ''} saved file."
                    )

            logger.info(
                f"Performing the computation to generate {self.pipe_name}{'.' + extra if extra else ''}. Hold tight."
            )
            return function(session_details, *args, **kwargs)

        return wrap

    def _save_after_generate_wrapper(
        self, function: Callable, disk_object: "BaseDiskObject"
    ):
        # decorator to load instead of calculating if not refreshing and saved data exists
        @wraps(function)
        def wrap(session, *args, **kwargs):
            logger = logging.getLogger("save_pipeline")

            kwargs = kwargs.copy()
            extra = kwargs.get("extra", "")
            version = kwargs.get("version", "")
            save_pipeline = kwargs.pop("save_pipeline", True)

            result = function(session, *args, **kwargs)
            if session is not None:
                if save_pipeline:
                    # we overwrite inside saver, if file exists and save_pipeline is True
                    disk_object.save(result)
                    # self.pipe.file_saver(session, result, extra=extra, version=version)
            else:
                logger.warning(
                    f"Cannot guess data saving location for {self.pipe_name}: 'session_details' argument must be supplied."
                )
            return result

        return wrap
