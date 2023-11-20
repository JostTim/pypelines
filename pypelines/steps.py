from functools import wraps, partial, update_wrapper
from .loggs import loggedmethod
from .arguments import autoload_arguments
import logging, inspect

from dataclasses import dataclass

from types import MethodType
from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .pipelines import Pipeline
    from .pipes import BasePipe
    from .disk import BaseDiskObject


def stepmethod(requires=[], version=None, do_dispatch=True):
    # This  allows method  to register class methods inheriting of BasePipe as steps.
    # It basically just step an "is_step" stamp on the method that are defined as steps.
    # This stamp will later be used in the metaclass __new__ to set additionnal usefull attributes to those methods
    def registrate(function: Callable):
        function.requires = [requires] if not isinstance(requires, list) else requires
        function.is_step = True
        function.version = version
        function.do_dispatch = do_dispatch
        function.step_name = function.__name__
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

        # we attach the values of the worker elements to BaseStep as they are get only (no setter) on worker (bound method)
        self.do_dispatch = self.worker.do_dispatch
        self.version = self.worker.version
        self.requires = self.worker.requires
        self.step_name = self.worker.step_name

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
                self.pipe.dispatcher(
                    loggedmethod(self.generation_mechanism), "generator"
                ),
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
            run_requirements=False,
            save_output=True,
            **kwargs,
        ):
            """
            skip=False
                if True, that step doesn't gets loaded if it is found on the drive, and just gets a return None. It cannot be set to True at the same time than refresh.
            refresh=False
                if True, that step's value gets refreshed instead of used from a file, even if there is one.
            refresh_requirements=False,
                if True, all the requirements are also refreshed. If false, no requirement gets refreshed. If a list of strings, the steps/pipes matching names are refreshed, and not the other ones. It doesn't refresh the current step, even if the name of the current step is inside the strings. For that, use refresh = True.
                Note that the behaviour in case a file exists for the current step level and we set refresh_requirements to something else than False, is that the file's content is returned ( if not skip, otherwise we just return None ), and we don't run any requirement.
                To force the refresh of current step + prior refresh of requirements, we would need to set refresh to True and refresh_requirements to True or list of strings.
            run_requirements=False,
                if True, the requirements are checked with skip = True, to verify that they exist on drive, and get generated otherwise. This is automatically set to true if refresh_requirements is not False.
            save_output=True,
                if False, we don't save the output to file after calculation. If there is not calculation (file exists and refresh is False), this has no effect. If True, we save the file after calculation.
            """

            logger = logging.getLogger(f"gen.{self.full_name}")

            if extra is None:
                extra = self.get_default_extra()

            self.pipeline.resolve()

            if refresh and skip:
                raise ValueError(
                    """You tried to set refresh (or refresh_main_only) to True and skipping to True simultaneouly. 
                    Stopped code to prevent mistakes : You probably set this by error as both have antagonistic effects. 
                    (skipping passes without loading if file exists, refresh overwrites after generating output if file exists) 
                    Please change arguments according to your clarified intention."""
                )

            if refresh_requirements:
                # if skip is True, and refresh_requirements is not None, we still make it possible, so that you can reprocess only if the file doen't exist
                run_requirements = True

            disk_object = self.get_disk_object(session, extra)

            # this is a flag to skip after checking the requirement tree if skip is True and data is loadable
            skip_after_tree = False

            if not refresh:
                if disk_object.is_loadable():
                    if disk_object.step_level_too_low():
                        logger.info(
                            f"File(s) have been found but with a step too low in the requirement stack. Reloading the generation tree"
                        )
                        run_requirements = True

                    elif disk_object.version_deprecated():
                        logger.info(
                            f"File(s) have been found but with an old version identifier. Reloading the generation tree"
                        )
                        run_requirements = True

                    elif skip:
                        logger.info(
                            f"File exists for {self.full_name}{'.' + extra if extra else ''}. Loading and processing will be skipped"
                        )
                        if not run_requirements or refresh_requirements != False:
                            return None

                        # if we should skip but run_requirements is True, we just postpone the skip to after triggering the requirement tree
                        skip_after_tree = True

                    # if nor step_level_too_low, nor version_deprecated, nor skip, we load the is_loadable disk object
                    else:
                        logger.info(f"Found data. Trying to load it")

                        try:
                            result = disk_object.load()
                        except IOError as e:
                            raise IOError(
                                f"The DiskObject responsible for loading {self.full_name} has `is_loadable() == True` but the loading procedure failed. Double check and test your DiskObject check_disk and load implementation. Check the original error above."
                            ) from e

                        logger.info(
                            f"Loaded {self.full_name}{'.' + extra if extra else ''} sucessfully."
                        )
                        return result
                else:
                    logger.info(
                        f"Could not find or load {self.full_name}{'.' + extra if extra else ''} saved file."
                    )
            else:
                logger.info(
                    f"`refresh` was set to True, ignoring the state of disk files and running the function."
                )

            if run_requirements:
                if refresh_requirements:
                    # if we want to regenerate all, we start from the bottom of the requirement stack and move up,
                    # forcing generation with refresh true on all the steps along the way.

                    for step in self.requirement_stack():
                        if self.pipe.pipe_name == step.pipe.pipe_name:
                            _extra = extra
                        else:
                            _extra = step.pipe.default_extra

                        # if refresh_requirements is not True but a list of things we should refresh, we parse it here
                        _refresh = True
                        if isinstance(refresh_requirements, list):
                            _refresh = (
                                True
                                if step.pipe_name
                                in refresh_requirements  # todo : improve the matching system a bit better in case step names collide ?
                                or step.full_name in refresh_requirements
                                else False
                            )
                        # if the step is not refreshed, we skip it so that run_requirements doesn't trigger if it is found and we don't load the data (process goes faster this way)
                        _skip = not _refresh

                        step.generate(
                            session,
                            run_requirements=True,
                            refresh=_refresh,
                            extra=_extra,
                            skip=_skip,
                        )

                else:
                    # if we run_requirements but don't

                    # if we want to only run requirements that would eventually be missing, we simply use the
                    # generation mechanism on the direct requirements, and skip if we have them generated already.
                    # the implementation will make them go down recursively to the bottom,
                    # by making them checking their direct requirements with run_requirements = True, etc...
                    # The only issue with this strategy is that is there is multiple redundant requirement information,
                    # then the checks will be executed multiple times.
                    # (The policy is that optimizing their requirement graph is the responsability of the user,
                    # using the provided graph visualization tool, otherwise the implementation would be rendered over-complex)
                    # TODO : in fact, this implementation could be as simple as running the reversed(requirement_stack) with run_requirements = False and skip = True....?

                    # for step in reversed(self.requirement_stack()) :
                    #     logger.info(
                    #         f"Running requirement {step.full_name}"
                    #     )
                    #     step.generate(session, skip = True, run_requirements = False)

                    for step in self.requires:
                        logger.info(f"Running requirement {step.full_name}")
                        # check via generate **direct** steps recursively before continuing with current step run.
                        # If they are present, recursive checks will not be run
                        if self.pipe.pipe_name == step.pipe.pipe_name:
                            _extra = extra
                        else:
                            _extra = step.pipe.default_extra
                        step.generate(
                            session, skip=True, run_requirements=True, extra=_extra
                        )

            if skip_after_tree:
                return None

            logger.info(
                f"Performing the computation to generate {self.full_name}{'.' + extra if extra else ''}. Hold tight."
            )
            result = self.pipe.pre_run_wrapper(
                self.worker(session, *args, extra=extra, **kwargs)
            )

            if save_output:
                logger.info(
                    f"Saving the generated {self.full_name}{'.' + extra if extra else ''} output."
                )
                disk_object.save(result)
            return result

        original_signature = inspect.signature(self.worker)
        original_params = list(original_signature.parameters.values())

        kwarg_position = len(original_params)

        if any([p.kind == p.VAR_KEYWORD for p in original_params]):
            kwarg_position = kwarg_position - 1

        # Create new parameters for the generation arguments and add them to the list
        new_params = [
            inspect.Parameter("skip", inspect.Parameter.KEYWORD_ONLY, default=False),
            inspect.Parameter("refresh", inspect.Parameter.KEYWORD_ONLY, default=False),
            inspect.Parameter(
                "run_requirements", inspect.Parameter.KEYWORD_ONLY, default=False
            ),
            inspect.Parameter(
                "save_output", inspect.Parameter.KEYWORD_ONLY, default=True
            ),
        ]

        # inserting the new params before the kwargs param if there is one.
        original_params = (
            original_params[:kwarg_position]
            + new_params
            + original_params[kwarg_position:]
        )

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
            raise ValueError(f"Parameter extra does not have a default value")
        return param.default

    def load_requirement(self, pipe_name, session, extra):
        try:
            req_step = [
                step for step in self.requirement_stack() if step.pipe_name == pipe_name
            ][-1]
        except IndexError as e:
            raise IndexError(
                f"Could not find a required step with the pipe_name {pipe_name} for the step {self.full_name}. Are you sure it figures in the requirement stack ?"
            ) from e
        return req_step.load(session, extra=extra)


@dataclass
class StepLevel:
    def __init__(self, step):
        self.requires = self.instanciate(step.requires)
        self.pipe_name = step.pipe_name

    def instanciate(self, requirements):
        new_req = []
        for req in requirements:
            req = StepLevel(req)
            new_req.append(req)
        return new_req

    def resolve_level(self, selfish=False):
        # if selfish is True, it gets transformed to the instance of StepLevel and gets passed down to the rest.
        # we also set substract to 1 to remember to remove 1 at the end of the uppermost resolve_level, to stay 0 based
        if selfish != False and selfish == True:
            selfish = self
            substract = 1
        # if selfish is False, it get passed down as a False to everything, and never changes value
        # in that case, we don't need to remove 1 at the end.
        else:
            substract = 0

        # if we are in selfish mode and found requirements but we are not currentely in a step that has the same pipe as the uppermost step on wich resolve_level is called, we don't increment level values
        if selfish != False and selfish.pipe_name != self.pipe_name:
            add = 0
        # otherwise, we add one at the end of the requirement stack for that step
        else:
            add = 1

        levels = []
        for req in self.requires:
            levels.append(req.resolve_level(selfish))

        # we only add values if that step has at least one requirement
        if len(levels) == 0:
            return 0

        return max(levels) + add - substract
