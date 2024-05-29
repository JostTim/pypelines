from .steps import BaseStep
from .multisession import BaseMultisessionAccessor
from .sessions import Session
from .disk import BaseDiskObject

from functools import wraps
import inspect, hashlib

from pandas import DataFrame

from abc import ABCMeta, abstractmethod

from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING, Literal, Dict
from types import MethodType

if TYPE_CHECKING:
    from .pipelines import Pipeline


class BasePipeType(Protocol):

    def __getattr__(self, name: str) -> "BaseStep": ...


class BasePipe(BasePipeType, metaclass=ABCMeta):
    # this class must implements only the logic to link steps together.

    default_extra = None

    # single_step: bool = False  # a flag to tell the initializer to bind the unique step of this pipe in place
    # of the pipe itself, to the registered pipeline.
    step_class: Type[BaseStep] = BaseStep
    disk_class: Type[BaseDiskObject] = BaseDiskObject
    multisession_class: Type[BaseMultisessionAccessor] = BaseMultisessionAccessor

    steps: Dict[str, BaseStep]

    def __init__(self, parent_pipeline: "Pipeline") -> None:
        """Initialize the Pipeline object with the parent pipeline and set up the steps based on the methods decorated
        with @stepmethod.

        Args:
            parent_pipeline (Pipeline): The parent pipeline object.

        Raises:
            ValueError: If no step class is registered with @stepmethod decorator, or if single_step is set to
                True with more than one step, or if steps are not linked in hierarchical order.

        Notes:
            - The step methods must inherit from BaseStep.
            - The steps should be linked in hierarchical order with `requires` specification for at least N-1 steps
                in a single pipe.

        Syntaxic sugar:
            - If the pipe is a single step, accessing any pipe instance in the pipeline can be done by iterating on
                pipeline.pipes.pipe.

        Attributes:
            pipeline (Pipeline): The parent pipeline object.
            pipe_name (str): The name of the pipeline.
            steps (Dict[str, BaseStep]): Dictionary containing the step objects.
            pipe: A reference to the pipeline object.

        Returns:
            None
        """
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
        """Return a hash representing the versions of all steps in the object.

        Returns:
            str: A 7-character hexadecimal hash representing the versions of all steps.
        """
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
        """Get the levels of each step in the pipeline.

        Args:
            selfish (bool, optional): Flag to indicate if the levels should be calculated selfishly. Defaults to True.

        Returns:
            dict: A dictionary containing the steps as keys and their corresponding levels as values.

        Raises:
            ValueError: If there are multiple steps with the same level and the saving backend doesn't
                support multi-step version identification.
        """
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
        """Return a string representation of the PipeObject in the format: "<BaseClassName.pipe_name PipeObject>".

        Returns:
            str: A string representation of the PipeObject.
        """
        return f"<{self.__class__.__bases__[0].__name__}.{self.pipe_name} PipeObject>"

    # @abstractmethod
    # def disk_step(self, session : Session, extra = "") -> BaseStep :
    #     #simply returns the pipe's (most recent in the step requirement order)
    # step instance that corrresponds to the step that is found on the disk
    #     return None

    def dispatcher(self, function: Callable, dispatcher_type):
        """Dispatches the given function based on the dispatcher type.

        Args:
            function (Callable): The function to be dispatched.
            dispatcher_type: The type of dispatcher to be used.

        Returns:
            Callable: A wrapped function based on the dispatcher type.
        """
        # the dispatcher must be return a wrapped function
        return function

    def pre_run_wrapper(self, function: Callable):
        """Return a wrapped function by the dispatcher."""
        # the dispatcher must be return a wrapped function
        return function

    def load(self, session, extra="", which: Literal["lowest", "highest"] = "highest"):
        """Load a step object for a session with optional extra data.

        Args:
            session: The session object to load the step for.
            extra (str, optional): Additional data to pass to the step object. Defaults to "".
            which (Literal["lowest", "highest"], optional): Determines whether to load the lowest or highest step.
                Defaults to "highest".

        Returns:
            The loaded step object.

        Raises:
            ValueError: If no matching step object is found for the session.
        """
        if which == "lowest":
            reverse = False
        else:
            reverse = True

        ordered_steps = sorted(
            list(self.steps.values()), key=lambda item: item.get_level(selfish=True), reverse=reverse
        )

        highest_step = None

        if isinstance(session, DataFrame):
            # if multisession, we assume we are trying to just load sessions
            # that all have reached the same level of requirements. (otherwise, use generate)
            # because of that, we use only the first session in the lot to search the highest loadable step
            search_on_session = session.iloc[0]
        else:
            search_on_session = session

        for step in ordered_steps:
            if step.get_disk_object(search_on_session, extra).is_matching():
                highest_step = step

        if highest_step is not None:  # if we found one : it is not None
            # we use the load wrapper, wich will dispatch to multissession or not automatically,
            # depending on session type (Series or DataFrame)
            return highest_step.load(session, extra)

        raise ValueError(f"Could not find a {self} object to load for the session {session.alias} with extra {extra}")
