from . step import BaseStep
from . multisession import BaseMultisessionAccessor

from functools import wraps

from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .pipeline import BasePipeline

class OutputData(Protocol):
    """Can be a mapping, iterable, single element, or None.

    This class is defined for typehints, and is not a real class useable at runtime"""

class PipeMetaclass(type):
    
    def __new__(cls : Type, pipe_name : str, bases : Iterable[Type], attributes : dict) -> Type:
        print(pipe_name, attributes)
        attributes["pipe_name"] = pipe_name
        
        steps = {}
        # this loop allows to populate cls.steps from the unistanciated the step methods of the cls.
        for name, attribute in attributes.items():
            if getattr(attribute, "is_step", False):
                steps[name] = PipeMetaclass.make_step_attributes(attribute , pipe_name , name)
        
        attributes["steps"] = steps

        if len(attributes["steps"]) > 1 and attributes["single_step"]:
            raise ValueError(f"Cannot set single_step to True if you registered more than one step inside {pipe_name} class")
        
        return super().__new__(cls, pipe_name, bases, attributes)

    @staticmethod
    def make_step_attributes(step : Callable, pipe_name : str, step_name : str) -> Callable:
        print(f"init of {pipe_name}")
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

        self._make_wrapped_functions()

    def _make_wrapped_functions(self):
        self.make_wrapped_save()
        self.make_wrapped_load()

    def __repr__(self) -> str:
        return f"<{self.__class__.__bases__[0].__name__}.{self.pipe_name} PipeObject>"

    def make_wrapped_save(self):
        self.save = self.dispatcher(self.file_saver)

    def make_wrapped_load(self):
        self.load = self.dispatcher(self.file_loader)

    def file_getter(self, session, extra, version) -> OutputData :
        #finds file, opens it, and return data.
        #if it cannot find the file, it returns a IOError
        ...
        #it will get

    def _check_version(self, step_name , found_version):
        #checks the found_version of the file is above or equal in the requirement order, to the step we are looking for
        #TODO
        self.pipeline.get_requirement_stack(step_name)

    def step_version(self, step):
        #simply returns the current string of the version that is in .
        ...
        
    def disk_version(self, session, extra) -> str :
        #simply returns the version string of the file(s) that it found.
        ...
        
    def file_saver(self, session, dumped_object, extra, version ):
        ...

    def file_loader(self, session, extra, version ):
        ...
        
    def file_checker(self, session):
        ...
        
    def dispatcher(self, function):
        # the dispatcher must be return a wrapped function
        ...

