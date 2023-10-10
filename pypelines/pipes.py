from .steps import BaseStep
from .multisession import BaseMultisessionAccessor
from .sessions import Session
from .disk import BaseDiskObject

from functools import wraps
import inspect

from abc import ABCMeta, abstractmethod

from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .pipelines import Pipeline
    
class BasePipe(metaclass = ABCMeta):
    # this class must implements only the logic to link steps together.

    single_step = False # a flag to tell the initializer to bind the unique step of this pipe in place of the pipe itself, to the registered pipeline.
    step_class = BaseStep 
    disk_class = BaseDiskObject
    multisession_class = BaseMultisessionAccessor

    def __init__(self, parent_pipeline : "Pipeline") -> None :

        self.multisession = self.multisession_class(self)
        self.pipeline = parent_pipeline
        self.pipe_name = self.__class__.__name__

        self.steps = {}
        # this loop populates self.steps dictionnary from the instanciated (bound) step methods.
        for (step_name, step) in inspect.getmembers( self , predicate = inspect.ismethod ):
            if getattr(step, "is_step", False): 
                self.steps[step_name] = step

        if len(self.steps) < 1 :
            raise ValueError(f"You should register at least one step class with @stepmethod in {self.pipe_name} class. { self.steps = }")

        if len(self.steps) > 1 and self.single_step:
            raise ValueError(f"Cannot set single_step to True if you registered more than one step inside {self.pipe_name} class. { self.steps = }")
        
        number_of_steps_with_requirements = 0
        for step in self.steps.values():
            if len(step.requires) :
                number_of_steps_with_requirements += 1
        
        if number_of_steps_with_requirements < len(self.steps) - 1 :
            raise ValueError(f"Steps of a single pipe must be linked in hierarchical order : Cannot have a single pipe with N steps (N>1) and have no `requires` specification for at least N-1 steps.")

        # this loop populates self.steps and replacs the bound methods with usefull Step objects. They must inherit from BaseStep
        for step_name, step in self.steps.items():
            step = self.step_class(self.pipeline, self, step)#, step_name)
            self.steps[step_name] = step #replace the bound_method by a step_class using that bound method, so that we attach the necessary components to it.
            setattr(self, step_name, step)

        # below is just a syntaxic sugar to help in case the pipe is "single_step" 
        # so that we can access any pipe instance in pipeline with simple iteration on 
        # pipeline.pipes.pipe, whatever if the object in pipelines.pipes is a step or a pipe
        self.pipe = self 

    def get_steps_levels(self):
        levels = {}
        for step in self.steps.values():
            levels[step] = step.get_level(selfish = True)
        return levels
 
    def __repr__(self) -> str:
        return f"<{self.__class__.__bases__[0].__name__}.{self.pipe_name} PipeObject>"

    @abstractmethod
    def disk_step(self, session : Session, extra = "") -> BaseStep :
        #simply returns the pipe's (most recent in the step requirement order) step instance that corrresponds to the step that is found on the disk
        return None

    def dispatcher(self, function : Callable):
        # the dispatcher must be return a wrapped function
        return function

    def pre_run_wrapper(self, function : Callable):
        # the dispatcher must be return a wrapped function
        return function

