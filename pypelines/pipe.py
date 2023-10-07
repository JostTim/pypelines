from . step import BaseStep
from . multisession import BaseMultisessionAccessor
from . sessions import Session

from functools import wraps
import inspect

from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .pipeline import BasePipeline

class PipeMetaclass(type):
    
    def __new__(cls : Type, pipe_name : str, bases : Iterable[Type], attributes : dict) -> Type:
        return super().__new__(cls, pipe_name, bases, attributes)
    
    def __init__(cls : Type, pipe_name : str, bases : Iterable[Type], attributes : dict) -> None:
        
        steps = getattr(cls,"steps",{})

        for name, attribute in attributes.items():
            if getattr(attribute, "is_step", False):
                steps[name] = PipeMetaclass.step_with_attributes(attribute , pipe_name , name)

        setattr(cls,"steps",steps)

    @staticmethod
    def step_with_attributes(step : BaseStep, pipe_name : str, step_name : str) -> BaseStep:
        
        setattr(step, "pipe_name", pipe_name) 
        setattr(step, "step_name", step_name) 

        return step
    
class BasePipe(metaclass = PipeMetaclass):
    # this class must implements only the logic to link blocks together.
    # It is agnostic about what way data is stored, and the way the blocks function. 
    # Hence it is designed to be overloaded, and cannot be used as is.

    use_versions = True
    single_step = False
    step_class = BaseStep
    multisession_class = BaseMultisessionAccessor

    def __init__(self, parent_pipeline : "BasePipeline") -> None :

        self.multisession = self.multisession_class(self)
        self.pipeline = parent_pipeline
        self.pipe_name = self.__class__.__name__
        print(self.pipe_name)

        if len(self.steps) > 1 and self.single_step:
            raise ValueError(f"Cannot set single_step to True if you registered more than one step inside {self.pipe_name} class")
        
        # this loop allows to populate self.steps from the now instanciated version of the step method.
        # Using only instanciated version is important to be able to use self into it later, 
        # without confusing ourselved with uninstanciated versions in the steps dict

        for step_name, _ in self.steps.items():
            step = getattr(self , step_name) # get the instanciated step method from name. 
            step = self.step_class(self.pipeline, self, step, step_name)
            self.steps[step_name] = step
            setattr(self, step_name, step)

        # attaches itself to the parent pipeline
        if self.single_step :
            step = list(self.steps.values())[0]
            self.pipeline.pipes[self.pipe_name] = step
            setattr(self.pipeline, self.pipe_name, step)
        else :
            self.pipeline.pipes[self.pipe_name] = self
            setattr(self.pipeline, self.pipe_name, self)

    def __repr__(self) -> str:
        return f"<{self.__class__.__bases__[0].__name__}.{self.pipe_name} PipeObject>"

    def _check_version(self, step_name , found_version):
        #checks the found_version of the file is above or equal in the requirement order, to the step we are looking for
        #TODO, need to intelligently think about how to implement this to be easily moduleable
        self.pipeline.get_requirement_stack(step_name)

    def identify_disk_version(self, version_string : str) -> BaseStep :
        """It will be called if the current version and disk version do not match
        
        This function returns True for OK, you can load this tep, it is a valid one,
        or False for no, not a valid version, you need to relaunch the requires chain list to regenerate from the last step that was in an ok version.

        If the disk version is a non deprecated version : 
            - check if that step is above the current version, and in that case, returns True, else returns False.

        If the disk version is a deprecated version :
            - directly reruns the requires chain list.

        """

    def disk_step(self, session : Session, extra = "") -> BaseStep :
        #simply returns the pipe's (most recent in the step requirement order) step instance that corrresponds to the step that is found on the disk
        return None
        ...
        
    def file_saver(self, session : Session, dumped_object, step : BaseStep, extra = "") -> None:
        ...

    def file_loader(self, session : Session, step : BaseStep, extra =  "") :
        """Loads a file that corresponds to the pipe, the step and the eventual version
        required (if step.use_version is True and step.version == the version of the file on the drive)
        If the version is not """
        ...

    def dispatcher(self, function : Callable):
        # the dispatcher must be return a wrapped function
        return function

    def pre_run_wrapper(self, function : Callable):
        # the dispatcher must be return a wrapped function
        return function

