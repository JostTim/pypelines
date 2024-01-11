from functools import wraps, partial, update_wrapper
from .loggs import loggedmethod, NAMELENGTH
from .arguments import autoload_arguments
import logging, inspect

from dataclasses import dataclass

from types import MethodType
from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .pipelines import Pipeline
    from .pipes import BasePipe
    from .disk import BaseDiskObject


def stepmethod(requires=[], version=None, do_dispatch=True, on_save_callbacks=[]):
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
        function.requires = [requires] if not isinstance(requires, list) else requires
        function.is_step = True
        function.version = version
        function.do_dispatch = do_dispatch
        function.step_name = function.__name__
        function.callbacks = [on_save_callbacks] if not isinstance(on_save_callbacks, list) else on_save_callbacks
        return function

    return registrate


class BaseStep:
    def __init__(
        self,
        pipeline: "Pipeline",
        pipe: "BasePipe",
        worker: MethodType,
    ):
        # save an instanciated access to the pipeline parent
        self.pipeline = pipeline
        # save an instanciated access to the pipe parent
        self.pipe = pipe
        # save an instanciated access to the step function (undecorated)
        self.worker = worker

        # we attach the values of the worker elements to BaseStep
        # as they are get only (no setter) on worker (bound method)
        self.do_dispatch = self.worker.do_dispatch
        self.version = self.worker.version
        self.requires = self.worker.requires
        self.step_name = self.worker.step_name

        self.callbacks = self.worker.callbacks

        self.worker = MethodType(worker.__func__, self)

        # self.make_wrapped_functions()

        update_wrapper(self, self.worker)
        # update_wrapper(self.generate, self.worker)

        self.multisession = self.pipe.multisession_class(self)

    @property
    def requirement_stack(self):
        return partial(self.pipeline.get_requirement_stack, instance=self)

    @property
    def pipe_name(self):
        return self.pipe.pipe_name

    @property
    def full_name(self):
        return f"{self.pipe_name}.{self.step_name}"

    @property
    def single_step(self):
        return self.pipe.single_step

    def disk_step(self, session, extra=""):
        disk_object = self.get_disk_object(session, extra)
        return disk_object.disk_step_instance()

    def __call__(self, *args, **kwargs):
        return self.worker(*args, **kwargs)

    def __repr__(self):
        return f"<{self.pipe_name}.{self.step_name} StepObject>"

    @property
    def load(self):
        return self.get_load_wrapped()

    @property
    def save(self):
        return self.get_save_wrapped()

    @property
    def generate(self):
        return self.get_generate_wrapped()

    # def make_wrapped_functions(self):
    #     self.save = self.make_wrapped_save()
    #     self.load = self.make_wrapped_load()
    #     self.generate = self.make_wrapped_generate()

    def get_save_wrapped(self):
        @wraps(self.pipe.disk_class.save)
        def wrapper(session, data, extra=None):
            if extra is None:
                extra = self.get_default_extra()
            self.pipeline.resolve()
            disk_object = self.get_disk_object(session, extra)
            return disk_object.save(data)

        if self.do_dispatch:
            return self.pipe.dispatcher(wrapper, "saver")
        return wrapper

    def get_load_wrapped(self):
        @wraps(self.pipe.disk_class.load)
        def wrapper(session, extra=None):
            # print("extra in load wrapper : ", extra)
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
        if self.do_dispatch:
            return autoload_arguments(
                self.pipe.dispatcher(loggedmethod(self.generation_mechanism), "generator"),
                self,
            )
        return autoload_arguments(loggedmethod(self.generation_mechanism), self)

    def get_level(self, selfish=False):
        self.pipeline.resolve()
        return StepLevel(self).resolve_level(selfish=selfish)

    def get_disk_object(self, session, extra=None):
        if extra is None:
            extra = self.get_default_extra()
        return self.pipe.disk_class(session, self, extra)

    @property
    def generation_mechanism(self):
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
                logger = logging.getLogger(f"╰─>req.{self.full_name}"[:NAMELENGTH])
            else:
                logger = logging.getLogger(f"gen.{self.full_name}"[:NAMELENGTH])

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
                            f"File exists for {self.full_name}{'.' + extra if extra else ''}. Loading and processing"
                            " will be skipped"
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
                                f"The DiskObject responsible for loading {self.full_name} has `is_loadable() == True`"
                                " but the loading procedure failed. Double check and test your DiskObject check_disk"
                                " and load implementation. Check the original error above."
                            ) from e

                        logger.load(f"Loaded {self.full_name}{'.' + extra if extra else ''} sucessfully.")
                        return result
                else:
                    logger.load(f"Could not find or load {self.full_name}{'.' + extra if extra else ''} saved file.")
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
                                if step.pipe_name in refresh_requirements or step.full_name in refresh_requirements
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
                logger.header(f"Performing the requirement {self.full_name}{'.' + extra if extra else ''}")
            else:
                logger.header(f"Performing the computation to generate {self.full_name}{'.' + extra if extra else ''}")
            kwargs.update({"extra": extra})
            if self.is_refresh_in_kwargs():
                kwargs.update({"refresh": refresh})
            result = self.pipe.pre_run_wrapper(self.worker(session, *args, **kwargs))

            if save_output:
                logger.save(f"Saving the generated {self.full_name}{'.' + extra if extra else ''} output.")
                disk_object.save(result)

                # AFTER the saving has been done, if there is some callback function that should be run, we execute them
                # If an exception is thrown in a callback, the whole pipeline will stop, intentionnaly.
                # TODO an option could be added to catch, display and store exceptions tracebacks,
                # while allowing the pipeline to continue,
                # in case the callbacks are not absolutely necessary for the pipeline process. (ex, generate plots)
                for callback_data in self.callbacks:
                    arguments = {"session": session, "extra": extra, "pipeline": self.pipeline}
                    if isinstance(callback_data, tuple):
                        callback = callback_data[0]
                        overriding_arguments = callback_data[1]
                    else:
                        callback = callback_data
                        overriding_arguments = {}
                    arguments.update(overriding_arguments)
                    try:
                        callback(**arguments)
                    except Exception as e:
                        import traceback

                        traceback_msg = traceback.format_exc()
                        logger.error(f"The callback {callback} failed with error : {e}")
                        logger.error("Full traceback below :\n" + traceback_msg)

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

        return wrapper

    def get_default_extra(self):
        """Get default value of a function's parameter"""
        sig = inspect.signature(self.worker)
        param = sig.parameters.get("extra")
        if param is None:
            raise ValueError(f"Parameter extra not found in function {self.full_name}")
        if param.default is param.empty:
            raise ValueError("Parameter extra does not have a default value")
        return param.default

    def is_refresh_in_kwargs(self):
        sig = inspect.signature(self.worker)
        param = sig.parameters.get("refresh")
        if param is None:
            return False
        return True

    def load_requirement(self, pipe_name, session, extra=None):
        try:
            req_step = [step for step in self.requirement_stack() if step.pipe_name == pipe_name][-1]
        except IndexError as e:
            raise IndexError(
                f"Could not find a required step with the pipe_name {pipe_name} for the step {self.full_name}. Are you"
                " sure it figures in the requirement stack ?"
            ) from e
        return req_step.load(session, extra=extra)

    def set_arguments(self, session, **arguments):
        ...

    def get_arguments(self, session):
        ...

    def start_remotely(self, session, extra=None, **kwargs):
        if not self.pipeline.use_celery:
            raise NotImplementedError(
                "Cannot use this feature with a pipeline that doesn't have a celery cluster access"
            )
        from one import ONE

        connector = ONE(mode="remote", data_access_mode="remote")

        worker = self.pipeline.celery.app.tasks[self.full_name]
        task_dict = connector.alyx.rest(
            "tasks",
            "create",
            data={
                "session": session.name,
                "name": self.full_name,
                "arguments": kwargs,
                "status": "Waiting",
                "executable": self.pipeline.celery.app_name,
            },
        )

        response_handle = worker.delay(task_dict["id"], extra=extra)
        # launch the task on the server, and waits until available.
        return RemoteTask(task_dict, response_handle)


class RemoteTask:
    infos = None
    response = None

    def __init__(self, task_infos_dict, response_handle):
        self.infos = task_infos_dict
        self.response = response_handle


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

    def resolve_level(self, selfish: bool = False, uppermost=None):
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
