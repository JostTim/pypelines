from functools import wraps, partial, update_wrapper, cache
from .loggs import loggedmethod, NAMELENGTH, getLogger, PypelineLogger
from .arguments import autoload_arguments
from .utils import to_snake_case

import logging, inspect
from pandas import DataFrame
from dataclasses import dataclass

from types import MethodType
from typing import Callable, Type, Iterable, Protocol, List, TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from .pipelines import Pipeline
    from .pipes import BasePipe
    from .disk import BaseDiskObject
    from .tasks import BaseStepTaskManager


def stepmethod(requires=None, version=None, do_dispatch=None, on_save_callbacks=None, disk_class=None, step_name=None):
    """Wrapper to attach some attributes to a method of a pipeline's pipe. These methods are necessary to trigger the
    pipeline creation mechanism on that step_method after the pipe has been fully defined.

    Args:
        requires (list, optional): single string or list of strings corresponding to other pipeline steps needed.
            The other pipeline steps must belong to the same pipeline than the one of the step_method. Defaults to [].
        version (_type_, optional): version of the step method. Changing this from none to a text or number will
            result in previous saved outputs to be reprocessed upon generation check, so that you can more easily
            control what needs to be reprocessed if you change an important computation step, and the minimum needed,
            but no less, will be automatically reprocessed. Defaults to None.
        do_dispatch (bool, optional): Wether to perform dispatch mechanism on calls of load, save and generate,
            or not, for this step_method. Defaults to True.
        on_save_callbacks (list, optional): The save callbacks can be a single callable, or a list of callables.
            Additionnaly, independently if you supply a singloe of multiple callables,
            they can be a tuple of (callable, named_argument_dict) instead of a simple callable.
            The arguments in the dict will override arguments that would have been passed by the generation mechanism,
            such as session, extra and pipeline. Defaults to [].

    Returns:
        callable : the callable with extra attributes attached
    """

    # This  allows method  to register class methods inheriting of BasePipe as steps.
    # It basically just step an "is_step" stamp on the method that are defined as steps.
    # This stamp will later be used in the metaclass __new__ to set additionnal usefull attributes to those methods
    def registrate(function: Callable):
        """Registers a function as a step in a process.

        Args:
            function (Callable): The function to be registered as a step.

        Returns:
            Callable: The registered function with additional attributes such as 'requires', 'is_step', 'version',
                'do_dispatch', 'step_name', and 'callbacks'.
        """
        function.is_step = True

        if requires is not None:
            function.requires = requires
        if version is not None:
            function.version = version
        if do_dispatch is not None:
            function.do_dispatch = do_dispatch
        if step_name is not None:
            function.step_name = step_name
        if on_save_callbacks is not None:
            function.callbacks = on_save_callbacks
        if disk_class is not None:
            function.disk_class = disk_class
        return function

    return registrate


class BaseStep:

    step_name: str

    requires: List["BaseStep"] | List[str] | str
    version: str | int
    do_dispatch: bool
    callbacks: List[Callable]

    disk_class: "Type[BaseDiskObject]"

    task: "BaseStepTaskManager"
    worker: Callable
    pipe: "BasePipe"
    pipeline: "Pipeline"

    def __init__(self, pipeline: "Pipeline", pipe: "BasePipe", worker: Optional[MethodType] = None):
        """Initialize a BaseStep object.

        Args:
            pipeline (Pipeline): The parent pipeline object.
            pipe (BasePipe): The parent pipe object.
            worker (MethodType): The worker method associated with this step.

        Attributes:
            pipeline (Pipeline): An instance of the parent pipeline.
            pipe (BasePipe): An instance of the parent pipe.
            worker (MethodType): An instance of the worker method.
            do_dispatch: The do_dispatch attribute of the worker method.
            version: The version attribute of the worker method.
            requires: The requires attribute of the worker method.
            step_name: The step_name attribute of the worker method.
            callbacks: The callbacks attribute of the worker method.
            multisession: An instance of the multisession class associated with the pipe.
            task: The task manager created by the runner backend of the pipeline.
        """
        # save an instanciated access to the pipeline parent
        self.pipeline = pipeline
        # save an instanciated access to the pipe parent
        self.pipe = pipe

        self.step_name = self.get_step_name(worker)

        # save an instanciated access to the step function (undecorated)
        self.find_and_bind_worker(worker)

        # we attach the values of the worker elements to the Step
        # as they are get only (no setter) on worker if it is not None (bound method)
        self.do_dispatch = self.get_attribute_or_default("do_dispatch", True)

        self.version = self.get_attribute_or_default("version", None)

        self.requires = self.get_attribute_or_default("requires", [])
        self.requires = [self.requires] if not isinstance(self.requires, list) else self.requires

        self.disk_class = self.get_attribute_or_default("disk_class", getattr(self.pipe, "disk_class"))
        if self.disk_class is None:
            raise AttributeError(
                f"disk_class of step {self.step_name} should be : \n"
                " - defined through decorator\n"
                " - defined with the disk_class attribute of the Step\n"
                " - defined with the disk_class attribute of the Pipe class that the Step is bound to\n"
            )

        self.callbacks = self.get_attribute_or_default("callbacks", [])
        self.callbacks = [self.callbacks] if not isinstance(self.callbacks, list) else self.callbacks

        # self.make_wrapped_functions()

        self.multisession = self.pipe.multisession_class(self)

        self.task = self.pipeline.runner_backend.create_task_manager(self)

    def get_attribute_or_default(self, attribute_name: str, default: Any) -> Any:
        return getattr(self, attribute_name, getattr(self.worker, attribute_name, default))

    def find_and_bind_worker(self, worker_object: Optional[MethodType]):
        if not hasattr(self, "worker"):
            if worker_object is None:
                AttributeError(self.worker_unfindable_message)
            else:
                self.worker = MethodType(worker_object.__func__, self)
                update_wrapper(self, self.worker)
        # else worker is already bound

    @property
    def worker_unfindable_message(self):
        return (
            f"For the step : {self.pipe.pipe_name}.{getattr(self, 'step_name', '<unknown>')}, a worker method must "
            "be defined if created from a class"
        )

    def get_step_name(self, worker_object: Optional[MethodType]):
        if not hasattr(self, "worker"):
            if worker_object is None:
                raise AttributeError(self.worker_unfindable_message)
            step_name = getattr(self, "step_name", getattr(worker_object, "step_name", worker_object.__name__))
        else:
            step_name = self.get_attribute_or_default("step_name", self.__class__.__name__)
        step_name = to_snake_case(step_name)

        if not step_name:
            raise ValueError(f'Step name in {self.pipe.pipe_name} cannot be an empty string "" or None')

        return step_name

    @property
    def requirement_stack(self) -> Callable:
        """Return a partial function that calls the get_requirement_stack method of the pipeline
        attribute with the instance set to self.
        """
        return partial(self.pipeline.get_requirement_stack, instance=self)

    @property
    def pipe_name(self) -> str:
        """Return the name of the pipe."""
        return self.pipe.pipe_name

    @property
    def relative_name(self) -> str:
        """Return the relative name of the object by concatenating the pipe name and step name."""
        return f"{self.pipe_name}.{self.step_name}"

    @property
    def pipeline_name(self) -> str:
        """Return the name of the pipeline."""
        return self.pipe.pipeline.pipeline_name

    @property
    def complete_name(self) -> str:
        """Return the complete name by combining the pipeline name and relative name."""
        return f"{self.pipeline_name}.{self.relative_name}"

    def disk_step(self, session, extra=""):
        """Retrieve the disk object and return the disk step instance."""
        disk_object = self.get_disk_object(session, extra)
        return disk_object.disk_step_instance()

    def __call__(self, *args, **kwargs):
        """Call the worker method with the given arguments and keyword arguments."""
        return loggedmethod(self.worker)(*args, **kwargs)

    def __repr__(self):
        """Return a string representation of the StepObject in the format: "<pipe_name.step_name StepObject>"."""
        return f"<{self.pipe_name}.{self.step_name} StepObject>"

    @property
    def load(self):
        """Load data using the get_load_wrapped method."""
        return self.get_load_wrapped()

    @property
    def save(self):
        """Save the current state of the object.

        Returns:
            The saved state of the object.
        """
        return self.get_save_wrapped()

    @property
    def generate(self):
        """Return the result of calling the get_generate_wrapped method."""
        return self.get_generate_wrapped()

    @property
    def run_callbacks(self):
        return self.get_run_callbacks()

    def get_save_wrapped(self):
        """Returns a wrapped function that saves data using the disk class.

        This function wraps the save method of the disk class with additional functionality.

        Args:
            session: The session to use for saving the data.
            data: The data to be saved.
            extra: Additional information to be used during saving (default is None).

        Returns:
            The wrapped function that saves the data using the disk class.
        """

        @wraps(self.disk_class.save)
        def wrapper(session, data, extra=None):
            """Wrapper function to save data to disk.

            Args:
                session: The session object.
                data: The data to be saved.
                extra: Additional information (default is None).

            Returns:
                The result of saving the data to disk.
            """
            if extra is None:
                extra = self.get_default_extra()
            self.pipeline.resolve()
            disk_object = self.get_disk_object(session, extra)
            return disk_object.save(data)

        if self.do_dispatch:
            return self.pipe.dispatcher(wrapper, "saver")
        return wrapper

    def get_load_wrapped(self):
        """Get a wrapped function for loading disk objects.

        This function wraps the load method of the disk class with the provided session, extra, and strict parameters.

        Args:
            session: The session to use for loading the disk object.
            extra: Additional parameters for loading the disk object (default is None).
            strict: A boolean flag indicating whether to strictly load the disk object (default is False).

        Returns:
            The wrapped function for loading disk objects.
        """

        @wraps(self.disk_class.load)
        def wrapper(session, extra=None, strict=False) -> Any:
            """Wrapper function to load disk object with session and optional extra parameters.

            Args:
                session: The session to use for loading the disk object.
                extra (optional): Extra parameters to be passed for loading the disk object. Defaults to None.
                strict (bool, optional): Flag to indicate strict loading. Defaults to False.

            Returns:
                The loaded disk object.

            Raises:
                ValueError: If the disk object does not match and has a status message.
            """
            # print("extra in load wrapper : ", extra)
            if isinstance(session, DataFrame):
                return self.multisession.load(sessions=session, extras=extra)

            if extra is None:
                extra = self.get_default_extra()
            # print("extra in load wrapper after None : ", extra)
            self.pipeline.resolve()
            disk_object = self.get_disk_object(session, extra)
            if not disk_object.is_matching():
                raise ValueError(disk_object.get_status_message())
            return disk_object.load()

        if self.do_dispatch:
            return self.pipe.dispatcher(wrapper, "loader")
        return wrapper

    def get_generate_wrapped(self):
        """Return the wrapped generation mechanism with optional dispatching.

        Returns:
            The wrapped generation mechanism with optional dispatching.
        """
        if self.do_dispatch:
            return autoload_arguments(
                self.pipe.dispatcher(loggedmethod(self.generation_mechanism), "generator"),
                self,
            )
        return autoload_arguments(loggedmethod(self.generation_mechanism), self)

    def get_run_callbacks(self):
        def wrapper(session, extra=None, show_plots=True):

            if extra is None:
                extra = self.get_default_extra()

            logger = logging.getLogger("callback_runner")
            for callback_data in self.callbacks:
                arguments = {"session": session, "extra": extra, "pipeline": self.pipeline}
                if isinstance(callback_data, tuple):
                    callback = callback_data[0]
                    overriding_arguments = callback_data[1]
                else:
                    callback = callback_data
                    overriding_arguments = {}
                arguments.update(overriding_arguments)
                on_what = f"{session.alias}.{extra}" if extra else session.alias
                try:
                    logger.info(f"Running the callback {callback.__name__} on {on_what}")
                    callback(**arguments)
                except Exception as e:
                    import traceback

                    traceback_msg = traceback.format_exc()
                    logger.error(f"The callback {callback} failed with error : {e}")
                    logger.error(f"Full traceback below :\n{traceback_msg}")

        if self.do_dispatch:
            return self.pipe.dispatcher(wrapper, "callbacks")
        return wrapper

    def get_level(self, selfish=False) -> int:
        """Get the level of the step.

        Args:
            selfish (bool): Whether to calculate the level selfishly. Defaults to False.

        Returns:
            int: The level of the step.
        """
        self.pipeline.resolve()
        return StepLevel(self).resolve_level(selfish=selfish)

    def is_required(self):
        # TODO implement this (False if the step is not present in any other step' requirement stack, else True)
        raise NotImplementedError

    def get_disk_object(self, session, extra=None):
        """Return a disk object based on the provided session and optional extra parameters.

        Args:
            session: The session to use for creating the disk object.
            extra (optional): Additional parameters to be passed to the disk object. Defaults to None.

        Returns:
            Disk: A disk object created using the provided session and extra parameters.
        """
        if extra is None:
            extra = self.get_default_extra()
        return self.disk_class(session, self, extra)

    @property
    def generation_mechanism(self):
        """Generates a wrapper function for the given worker function with additional functionality such as skipping,
        refreshing, checking requirements, and saving output to file.

        Args:
            session: The session object.
            *args: Positional arguments for the worker function.
            extra: Additional argument for the worker function (default is None).
            skip: If True, the step doesn't get loaded if found on the drive (default is False).
            refresh: If True, the step's value gets refreshed instead of used from a file (default is False).
            refresh_requirements: If True, refreshes all requirements; if list of strings, refreshes specific
                steps/pipes (default is False).
            check_requirements: If True, checks requirements with skip=True (default is False).
            save_output: If False, doesn't save the output to file after calculation (default is True).
            **kwargs: Additional keyword arguments for the worker function.

        Returns:
            The wrapper function with extended functionality.
        """

        @wraps(self.worker)
        def wrapper(
            session,
            *args,
            extra=None,
            skip=False,
            refresh=False,
            refresh_requirements=False,
            check_requirements=False,
            save_output=True,
            **kwargs,
        ):
            """
            skip=False
                if True, that step doesn't gets loaded if it is found on the drive, and just gets a return None.
                It cannot be set to True at the same time than refresh.
            refresh=False
                if True, that step's value gets refreshed instead of used from a file, even if there is one.
            refresh_requirements=False,
                if True, all the requirements are also refreshed. If false, no requirement gets refreshed.
                If a list of strings, the steps/pipes matching names are refreshed, and not the other ones.
                It doesn't refresh the current step, even if the name of the current step is inside the strings.
                For that, use refresh = True.
                Note that the behaviour in case a file exists for the current step level and we set refresh_requirements
                to something else than False, is that the file's content is returned
                ( if not skip, otherwise we just return None ), and we don't run any requirement.
                To force the refresh of current step + prior refresh of requirements,
                we would need to set refresh to True and refresh_requirements to True or list of strings.
            check_requirements=False,
                if True, the requirements are checked with skip = True, to verify that they exist on drive,
                and get generated otherwise. This is automatically set to true if refresh_requirements is not False.
            save_output=True,
                if False, we don't save the output to file after calculation. If there is not calculation
                (file exists and refresh is False), this has no effect. If True, we save the file after calculation.
            """

            if extra is None:
                extra = self.get_default_extra()

            self.pipeline.resolve()

            in_requirement = kwargs.pop(
                "in_requirement", False
            )  # a flag to know if we are in requirement run or toplevel

            if in_requirement:
                logger = logging.getLogger(f"╰─>req.{self.relative_name}"[:NAMELENGTH])
            else:
                logger = logging.getLogger(f"gen.{self.relative_name}"[:NAMELENGTH])

            if refresh and skip:
                raise ValueError(
                    """You tried to set refresh (or refresh_main_only) to True and skipping to True simultaneouly.
                    Stopped code to prevent mistakes : You probably set this by error as both have antagonistic effects.
                    (skipping passes without loading if file exists,
                    refresh overwrites after generating output if file exists)
                    Please change arguments according to your clarified intention."""
                )

            if refresh_requirements:
                # if skip is True, and refresh_requirements is not None, we still make it possible,
                # so that you can reprocess only if the file doen't exist
                check_requirements = True

            disk_object = self.get_disk_object(session, extra)

            # this is a flag to skip after checking the requirement tree if skip is True and data is loadable
            skip_after_tree = False

            if not refresh:
                if disk_object.is_loadable():
                    if disk_object.step_level_too_low():
                        logger.load(
                            "File(s) have been found but with a step too low in the requirement stack. Reloading the"
                            " generation tree"
                        )
                        check_requirements = True

                    elif disk_object.version_deprecated():
                        logger.load(
                            "File(s) have been found but with an old version identifier. Reloading the generation tree"
                        )
                        check_requirements = True

                    elif skip:
                        logger.load(
                            f"File exists for {self.relative_name}{'.' + extra if extra else ''}."
                            " Loading and processing will be skipped"
                        )
                        if not check_requirements or refresh_requirements is not False:
                            return None

                        # if we should skip but check_requirements is True, we just postpone the skip to after
                        # triggering the requirement tree
                        # Note that or refresh_requirements != False means it does not trigger skip_after_tree in the
                        # case refresh_requirements is not False.
                        # This is to avoid the strange behaviour that with skip false, it wouldn't run requirements,
                        # and with skip true, it would.
                        # It would otherwise be counter intuitive given the fact that skip=True seem to imply we tend
                        # to avoid more steps while setting it to true than to false
                        skip_after_tree = True

                    # if not step_level_too_low, nor version_deprecated, nor skip, we load the is_loadable disk object
                    else:
                        logger.load("Found data. Trying to load it")

                        try:
                            result = disk_object.load()
                        except IOError as e:
                            raise IOError(
                                f"The DiskObject responsible for loading {self.relative_name}"
                                " has `is_loadable() == True`"
                                " but the loading procedure failed. Double check and test your DiskObject check_disk"
                                " and load implementation. Check the original error above."
                            ) from e

                        logger.load(f"Loaded {self.relative_name}{'.' + extra if extra else ''} sucessfully.")
                        return result
                else:
                    logger.load(
                        f"Could not find or load {self.relative_name}{'.' + extra if extra else ''} saved file."
                    )
            else:
                logger.load("`refresh` was set to True, ignoring the state of disk files and running the function.")

            if check_requirements:
                # if refresh_requirements:
                # if we want to regenerate all, we start from the bottom of the requirement stack and move up,
                # forcing generation with refresh true on all the steps along the way.
                logger.info("Checking the requirements")
                for step in self.requirement_stack():
                    if self.pipe.pipe_name == step.pipe.pipe_name:
                        _extra = extra
                    else:
                        _extra = step.pipe.default_extra

                    # by default, we don't refresh the step
                    _refresh = False

                    # if this is true, refresh_requirements is either True or a list of strings
                    if bool(refresh_requirements):
                        # then, by default, we refresh the step.
                        _refresh = True
                        # if refresh_requirements is not True but a list of steps we should refresh,
                        # we parse it here to not refresh it if it is not included in the list of strings
                        if isinstance(refresh_requirements, list):
                            _refresh = (
                                True
                                if step.pipe_name in refresh_requirements or step.relative_name in refresh_requirements
                                else False
                            )

                    # if the step is not refreshed, we skip it so that check_requirements doesn't trigger if
                    # it is found and we don't load the data (process goes faster this way)
                    _skip = not _refresh

                    step.generate(
                        session,
                        check_requirements=False,
                        refresh=_refresh,
                        extra=_extra,
                        skip=_skip,
                        in_requirement=True,
                    )

            if skip_after_tree:
                return None

            if in_requirement:
                logger.header(f"Performing the requirement {self.relative_name}{'.' + extra if extra else ''}")
            else:
                logger.header(
                    f"Performing the computation to generate {self.relative_name}{'.' + extra if extra else ''}"
                )
            kwargs.update({"extra": extra})
            if self.is_refresh_in_kwargs():
                kwargs.update({"refresh": refresh})
            result = self.pipe.pre_run_wrapper(self.worker(session, *args, **kwargs))

            if save_output:
                logger.save(f"Saving the generated {self.relative_name}{'.' + extra if extra else ''} output.")
                disk_object.save(result)
                self.run_callbacks(session, extra=extra, show_plots=False)

            return result

        original_signature = inspect.signature(self.worker)
        original_params = list(original_signature.parameters.values())

        kwarg_position = len(original_params)

        if any([p.kind == p.VAR_KEYWORD for p in original_params]):
            kwarg_position = kwarg_position - 1

        # Create new parameters for the generation arguments and add them to the list,
        # if they don't already exist in the step function declaration
        new_params = []
        for param, default_value in {
            "skip": False,
            "refresh": False,
            "refresh_requirements": False,
            "check_requirements": False,
            "save_output": True,
        }.items():
            if original_signature.parameters.get(param) is None:
                new_params.append(inspect.Parameter(param, inspect.Parameter.KEYWORD_ONLY, default=default_value))

        # inserting the new params before the kwargs param if there is one.
        original_params = original_params[:kwarg_position] + new_params + original_params[kwarg_position:]

        # Replace the wrapper function's signature with the new one
        wrapper.__signature__ = original_signature.replace(parameters=original_params)
        wrapper.__doc__ = self.generate_doc()

        return wrapper

    def generate_doc(self) -> str:
        """Generate a new docstring by inserting a chapter about Pipeline Args before the existing
        docstring of the function.
        If the existing docstring contains 'Raises' or 'Returns', the new chapter will be inserted before that.
        If not, it will be inserted at the end of the existing docstring.
        """

        new_doc = ""
        doc = self.worker.__doc__
        if doc is None:
            return new_doc
        lines = doc.split("\n")
        lines_count = len(lines)
        inserted_chapter = False
        new_chapter = """
            Pipeline Args:
                skip (bool, optional) : If True and the data can be loaded, it will be skipped instead
                    (to avoid lengthy load time if one only wants to generate an output for later)
                    Particularly usefull on a remove celery node where the result does not need to be returned,
                    for example.
                    Note that if it is True and that the result cannot be loaded, the generation mechanism
                    will of course happen, and the result will be saved if save_output is True
                    (usually, saved to a disk file).
                    Defaults to False.
                refresh (bool, optional) : If True, it forces the generation mechanism to happen, even if a valid disk
                    file can be found. Note that, because refresh forces the generation whatever the load state and
                    skip tries to avoid having to load, only if the generation does not occur, calling skip=True
                    when refresh is also True is pointless. For this reason and to avoid user confusion,
                    calling with both True at the same time raises and error (with an help message telling you
                    to set one of them to False)
                    Defaults to False
                refresh_requirements (str, List[str], optional) : If set to a string or list of strings,
                    the steps that have a pipe_name or relative_name matching one of the strings supplied get refreshed
                    in the requirement tree check stage. For example, setting refresh_requirements=["trials_df"]
                    will trigger a refresh on all steps of the pipe trials_df that are encountered during
                    requirement tree check. For more specificity, setting refresh_requirements=["trials_df.my_step"]
                    will only refresh the step my_step of the pipe trials_df when encountered in the requirement tree.
                    You can cumulate multiple refresh conditions by including several strings in this list.
                    Defaults to empty list.
                check_requirements (bool, optional) : If true, the requirement tree check stage will be triggered to
                    verify that the outputs of the steps required by the current call are already available.
                    If not, they will be generated and saved, before each stage in the requirement tree is run.
                    This should prevent errors or crashes due to requirements missing, and is the main desired feature
                    of the pypelines package.
                    It is set to False by default to avoid the users running into some issues in case they are starting
                    to use the package as possible data loss (processed data and not raw data if well used)
                    is at stake if user defined steps classes are misused / miscoded.
                    Defaults to False.
                save_output (bool, optional) : If True, once the data is obtained throught the generation mechanism
                    it is saved before being returned (if skip False).
                    (Data is usually saved to disk, but it depends on the disk_object implementation you selected,
                    and can be to a database or ram object serving as cache during a session, for example.)
                    If False, the data is not saved. This might be usefull especially during developpements tests,
                    if tested with real data, and results are already loadable but you don't want to erase it by setting
                    refresh = True.
                    Defaults to True.
        """
        for line_no, line in enumerate(lines):
            if not inserted_chapter and ("Raises" in line or "Returns" in line or line_no >= lines_count - 1):
                new_doc += new_chapter + "\n"
                inserted_chapter = True
            new_doc += line + "\n"
        return new_doc

    def get_default_extra(self):
        """Get default value of a function's parameter"""
        sig = inspect.signature(self.worker)
        param = sig.parameters.get("extra")
        if param is None:
            raise ValueError(f"Parameter extra not found in function {self.relative_name}")
        if param.default is param.empty:
            raise ValueError("Parameter extra does not have a default value")
        return param.default

    def is_refresh_in_kwargs(self):
        """Check if the 'refresh' parameter is present in the keyword arguments of the function.

        Returns:
            bool: True if the 'refresh' parameter is present, False otherwise.
        """
        sig = inspect.signature(self.worker)
        param = sig.parameters.get("refresh")
        if param is None:
            return False
        return True

    def load_requirement(self, pipe_name, session, extra=None, **kwargs) -> Any:
        """Load the specified requirement step for the given pipe name.

        Args:
            pipe_name (str): The name of the pipe for which the requirement step needs to be loaded.
            session: The session to be used for loading the requirement step.
            extra (optional): Any extra information to be passed while loading the requirement step.

        Returns:
            The loaded requirement step.

        Raises:
            IndexError: If the required step with the specified pipe name is not found in the requirement stack.
        """

        if pipe_name in kwargs.keys():
            return kwargs[pipe_name]

        try:
            req_step = [step for step in self.requirement_stack() if step.pipe_name == pipe_name][-1]
        except IndexError as e:
            raise IndexError(
                f"Could not find a required step with the pipe_name {pipe_name} for the step {self.relative_name}. "
                "Are you sure it figures in the requirement stack ?"
            ) from e
        return req_step.load(session, extra=extra)

    def set_arguments(self, session, **arguments):
        """Set the arguments for the session.

        Args:
            session: The session to set the arguments for.
            **arguments: Additional keyword arguments to set.

        Raises:
            NotImplementedError: This method is not implemented and should be overridden in a subclass.
        """
        raise NotImplementedError

    def get_arguments(self, session):
        """Get the arguments for the specified session.

        Args:
            self: The object instance.
            session: The session for which arguments need to be retrieved.

        Raises:
            NotImplementedError: This method must be implemented in a subclass.
        """
        raise NotImplementedError

    @property
    def logger(self) -> PypelineLogger:
        return getLogger(self.step_name[:NAMELENGTH])

    def __eq__(self, other_step: "BaseStep"):
        if self.complete_name == other_step.complete_name:
            return True
        return False

    def __lt__(self, other_step: "BaseStep"):  # Less than (<)
        if self.pipe != other_step.pipe:
            raise ArithmeticError("Cannot compare two steps of different pipes with <")
        return self.get_level(selfish=True) < other_step.get_level(selfish=True)

    def __le__(self, other_step: "BaseStep"):  # Less than or equal (<=)
        if self.pipe != other_step.pipe:
            raise ArithmeticError("Cannot compare two steps of different pipes with <=")
        return self.get_level(selfish=True) <= other_step.get_level(selfish=True)

    def __gt__(self, other_step: "BaseStep"):  # Greater than (>)
        if self.pipe != other_step.pipe:
            raise ArithmeticError("Cannot compare two steps of different pipes with >")
        return self.get_level(selfish=True) > other_step.get_level(selfish=True)

    def __ge__(self, other_step: "BaseStep"):  # Greater than or equal (>=)
        if self.pipe != other_step.pipe:
            raise ArithmeticError("Cannot compare two steps of different pipes with >=")
        return self.get_level(selfish=True) >= other_step.get_level(selfish=True)

    def __hash__(self) -> int:
        return hash(self.complete_name)


@dataclass
class StepLevel:
    """A class used to represent the level of a Step.
    This class helps track and manage step dependencies in a pipeline.

    Attributes:
        requires (list[StepLevel]): A list of step requirements.
        pipe_name (str): The name of the pipeline this step is a part of.
        step_name (str): The name of the step.

    Methods :
        instanciate(requirements: list): converts a list of requirements into instances of `StepLevel`.
        resolve_level(selfish=bool, uppermost=None): calculates and returns the level of a step.
    """

    def __init__(self, step):
        """Constructs all necessary attributes for the StepLevel object.

        Args:
            step (Step): A Step instance having attributes `pipe_name`, `step_name`, and `requires`.
        """
        self.requires = self.instanciate(step.requires)
        self.pipe_name = step.pipe_name
        self.step_name = step.step_name

    def instanciate(self, requirements):
        """
        Converts each item in the passed list to an instance of `StepLevel`.

        Args:
            requirements (list): A list of step requirements.

        Returns:
            list: A list of StepLevel instances representing step requirements.
        """
        new_req = []
        for req in requirements:
            req = StepLevel(req)
            new_req.append(req)
        return new_req

    def resolve_level(self, selfish: bool = False, uppermost=None) -> int:
        """Calculates and returns the "level" of the step.

        If `selfish` is True, only the requirements that are the same pipe as the `uppermost`
        will be considered, others won't increment the level values. If `selfish` is False,
        all requirements contribute to the step level.

        Args:
            selfish (bool, optional): A flag to specify if the StepLevel should count
                just the level of requirements that are also on the same pipe. Defaults to False.
            uppermost (StepLevel, optional): The uppermost level in the pipeline, defaults to self.

        Returns:
            int: The computed level of the step.
        """

        # if selfish is True, we only count the requirements that are the same pipe as the uppermost call

        if uppermost is None:
            uppermost = self

        if uppermost == self:
            add = 0

        else:
            if selfish and uppermost.pipe_name != self.pipe_name:
                # if we are in selfish mode
                # but we are not currentely in the same step as or a step that has the same pipe as the uppermost
                # step on wich resolve_level is called, we don't increment level values
                add = 0

            else:
                # otherwise, we add one at the end of the requirement stack for that step
                add = 1

        levels = []
        for req in self.requires:
            levels.append(req.resolve_level(selfish, uppermost))

        # we cannot calculate max of an empty list, so we add one 0 here in case there is no requirements
        if len(levels) == 0:
            levels = [0]

        return max(levels) + add

    def __eq__(self, value):
        """Checks if this step is equal to the provided value based on `pipe_name` and `step_name`.

        Args:
            value (StepLevel): Another StepLevel instance to compare with.

        Returns:
            bool: True if both pipe_name and step_name are equal, False otherwise.
        """
        try:
            if self.pipe_name == value.pipe_name and self.step_name == value.step_name:
                return True
            return False
        except AttributeError:
            return False
