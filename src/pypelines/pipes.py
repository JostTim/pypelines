from .steps import BaseStep
from .multisession import BaseMultisessionAccessor
from .sessions import Session
from .disk import BaseDiskObject
from .utils import to_snake_case

from functools import wraps
import inspect, hashlib

from pandas import DataFrame

from abc import ABCMeta, abstractmethod
from copy import deepcopy

from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING, Literal, Dict, Set
from types import MethodType

from logging import getLogger

if TYPE_CHECKING:
    from .pipelines import Pipeline


class BasePipeType(Protocol):

    def __getattr__(self, name: str) -> "BaseStep": ...


class BasePipe(BasePipeType, metaclass=ABCMeta):
    # this class implements only the logic to link steps together.

    default_extra = None

    # single_step: bool = False  # a flag to tell the initializer to bind the unique step of this pipe in place
    # of the pipe itself, to the registered pipeline.
    step_class: Type[BaseStep] = BaseStep
    disk_class: Type[BaseDiskObject] = BaseDiskObject
    multisession_class: Type[BaseMultisessionAccessor] = BaseMultisessionAccessor

    pipe_name: str

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
        logger = getLogger("Pipe")

        self.pipe_name = (
            to_snake_case(self.pipe_name)
            if getattr(self, "pipe_name", None) is not None
            else to_snake_case(self.__class__.__name__)
        )

        self.pipeline = parent_pipeline

        # pipeline.pipes.pipe will thus work whatever if the object in pipelines.pipes is a step or a pipe
        self.pipe = self

        step_methods: Set[MethodType] = set()

        # first strategy, scans decorated methods that belongs to the pipe, and have an is_step attribute
        # (decorated, directly in the pipe class)
        for attribute_name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if getattr(method, "is_step", False):
                step_methods.add(method)

        # second strategy, scans the Steps pipe class, if existing, and get all of it's methods to make steps.
        # (decorated or not, and grouped under Steps class, to differentiate them from non Step methods of the pipe)
        for attribute_name, class_object in inspect.getmembers(self, predicate=inspect.isclass):
            if attribute_name == "Steps":
                for sub_attribute_name, method in inspect.getmembers(class_object(), predicate=inspect.ismethod):
                    # if getattr(method, "is_step", False):
                    step_methods.add(method)
                break

        # third strategy, by checking BaseStep inheriting classes defined in the Pipe
        steps_classes: Set[Type[BaseStep]] = set()
        for attribute_name, class_object in inspect.getmembers(self, predicate=inspect.isclass):
            # The attribute Pipe.step_class is a child of BasePipe but we don't want to instantiate it as a step.
            # It is meant to be used as a contructor class for making steps out of methods,
            # #with first or second strategy
            if attribute_name == "step_class":
                continue
            if issubclass(class_object, BaseStep):
                steps_classes.add(class_object)

        if len(steps_classes) + len(step_methods) < 1:
            raise ValueError(
                f"You should register at least one step in the pipe:{self.pipe_name} "
                f"of the pipeline {self.pipeline.pipeline_name}. "
                f"{step_methods=}, {steps_classes=}."
            )

        self.steps = {}
        # We instanciate decorated methods steps using the step_class defined in the pipe
        for step_worker_method in step_methods:
            logger.debug(f"defining {step_worker_method} from method in {self}")
            instanciated_step = self.step_class(pipeline=self.pipeline, pipe=self, worker=step_worker_method)
            self.attach_step(instanciated_step)

        # We instanciate class based steps using their class directly
        for step_class in steps_classes:
            logger.debug(f"defining {step_class} from class in {self}")
            instanciated_step = step_class(pipeline=self.pipeline, pipe=self)
            self.attach_step(instanciated_step)

        self.verify_hierarchical_requirements()

    def verify_hierarchical_requirements(self):

        number_of_steps_with_requirements = len([True for step in self.steps.values() if len(step.requires) != 0])

        if number_of_steps_with_requirements < len(self.steps) - 1:
            raise ValueError(
                "Steps of a single pipe must be linked in hierarchical order : Cannot have a single pipe with N steps"
                " (N>1) and have no `requires` specification for at least N-1 steps."
            )

    def attach_step(self, instanciated_step: "BaseStep", rebind=False):

        if rebind:
            instanciated_step = deepcopy(instanciated_step)
            instanciated_step.pipeline = self.pipeline
            instanciated_step.pipe = self.pipe
            # TODO : eventually scan requirements strings / objects to rebind
            # TODO : them to the local pipeline correspunding objects

        if instanciated_step.step_name in self.steps.keys():
            raise AttributeError(
                "Cannot attach two steps of the same name via different methods to a single pipe."
                f" The step named {instanciated_step.step_name} is attached through two "
                "mechanisms or has two methods with conflicting names."
            )

        self.steps[instanciated_step.step_name] = instanciated_step
        setattr(self, instanciated_step.step_name, instanciated_step)
        self.pipeline.resolved = False

    @property
    def version(self) -> str:
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

    def ordered_steps(self, first: Literal["lowest", "highest"] = "lowest"):
        reverse = False if first == "lowest" else True
        return sorted(list(self.steps.values()), key=lambda item: item.get_level(selfish=True), reverse=reverse)

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

        ordered_steps = self.ordered_steps(first=which)

        highest_step = None

        if isinstance(session, DataFrame):
            # if multisession, we assume we are trying to just load sessions
            # that all have reached the same level of requirements. (otherwise, use generate to make them match levels)
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

    def __eq__(self, other_pipe: "BasePipe"):
        if hash(self) == hash(other_pipe):
            return True
        return False

    def __hash__(self):
        return hash(f"{self.pipeline.pipeline_name}.{self.pipe_name}")
