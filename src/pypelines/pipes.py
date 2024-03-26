from .steps import BaseStep
from .multisession import BaseMultisessionAccessor
from .sessions import Session
from .disk import BaseDiskObject

from functools import wraps
import inspect, hashlib

from abc import ABCMeta, abstractmethod

from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING, Literal, Dict
from types import MethodType

if TYPE_CHECKING:
    from .pipelines import Pipeline


class BasePipe(metaclass=ABCMeta):
    # this class must implements only the logic to link steps together.

    default_extra = None

    # single_step: bool = False  # a flag to tell the initializer to bind the unique step of this pipe in place
    # of the pipe itself, to the registered pipeline.
    step_class: Type[BaseStep] = BaseStep
    disk_class: Type[BaseDiskObject] = BaseDiskObject
    multisession_class: Type[BaseMultisessionAccessor] = BaseMultisessionAccessor

    steps: Dict[str, BaseStep]

    def __init__(self, parent_pipeline: "Pipeline") -> None:
        self.pipeline = parent_pipeline
        self.pipe_name = self.__class__.__name__

        _steps: Dict[str, MethodType] = {}
        # this loop populates self.steps dictionnary from the instanciated (bound) step methods.
        for step_name, step in inspect.getmembers(self, predicate=inspect.ismethod):
            if getattr(step, "is_step", False):
                _steps[step_name] = step

        if len(_steps) < 1:
            raise ValueError(
                f"You should register at least one step class with @stepmethod in {self.pipe_name} class. {_steps=}"
            )

        # if len(_steps) > 1 and self.single_step:
        #     raise ValueError(
        #         f"Cannot set single_step to True if you registered more than one step inside {self.pipe_name} class."
        #         f" { _steps = }"
        #     )

        number_of_steps_with_requirements = 0
        for step in _steps.values():
            if len(step.requires):
                number_of_steps_with_requirements += 1

        if number_of_steps_with_requirements < len(_steps) - 1:
            raise ValueError(
                "Steps of a single pipe must be linked in hierarchical order : Cannot have a single pipe with N steps"
                " (N>1) and have no `requires` specification for at least N-1 steps."
            )

        # this loop populates self.steps and replacs the bound methods with usefull Step objects.
        # They must inherit from BaseStep
        self.steps = {}
        for step_name, step in _steps.items():
            step = self.step_class(self.pipeline, self, step)  # , step_name)
            self.steps[step_name] = step  # replace the bound_method by a step_class using that bound method,
            # so that we attach the necessary components to it.
            setattr(self, step_name, step)

        # below is just a syntaxic sugar to help in case the pipe is "single_step"
        # so that we can access any pipe instance in pipeline with simple iteration on
        # pipeline.pipes.pipe, whatever if the object in pipelines.pipes is a step or a pipe
        self.pipe = self

    @property
    def version(self):
        versions = []
        for step in self.steps.values():
            versions.append(str(step.version))
        versions_string = "/".join(versions)

        m = hashlib.sha256()
        r = versions_string.encode()
        m.update(r)
        version_hash = m.hexdigest()[0:7]

        return version_hash

    def get_levels(self, selfish=True):
        levels = {}
        for step in self.steps.values():
            levels[step] = step.get_level(selfish=selfish)

        # if checking step levels internal to a single pipe,
        # we disallow several steps having identical level if the saving backend doesn't allow
        # for multi-step version identification
        if selfish and self.disk_class.step_traceback != "multi":
            # we make a set of all the values. if there is some duplicates,
            # the length of the set will be smaller than the levels dict
            if len(set(levels.values())) != len(levels):
                raise ValueError(
                    f"The disk backend {self.disk_class} does not support multi step (step_traceback attribute). All"
                    f" steps of the pipe {self.pipe_name} must then be hierarchically organized"
                )

        return levels

    def __repr__(self) -> str:
        return f"<{self.__class__.__bases__[0].__name__}.{self.pipe_name} PipeObject>"

    # @abstractmethod
    # def disk_step(self, session : Session, extra = "") -> BaseStep :
    #     #simply returns the pipe's (most recent in the step requirement order)
    # step instance that corrresponds to the step that is found on the disk
    #     return None

    def dispatcher(self, function: Callable, dispatcher_type):
        # the dispatcher must be return a wrapped function
        return function

    def pre_run_wrapper(self, function: Callable):
        # the dispatcher must be return a wrapped function
        return function

    def load(self, session, extra="", which: Literal["lowest", "highest"] = "highest"):
        if which == "lowest":
            reverse = False
        else:
            reverse = True

        ordered_steps = sorted(
            list(self.steps.values()), key=lambda item: item.get_level(selfish=True), reverse=reverse
        )

        for step in ordered_steps:
            if step.get_disk_object(session, extra).is_matching():
                return step.load(session, extra)
        raise ValueError(f"Could not find a {self} object to load for the session {session.alias} with extra {extra}")
