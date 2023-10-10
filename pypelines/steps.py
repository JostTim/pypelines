from functools import wraps, partial, update_wrapper
from .loggs import loggedmethod
import logging

from dataclasses import dataclass

from types import MethodType
from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .pipelines import BasePipeline
    from .pipes import BasePipe
    from .disk import BaseDiskObject

def stepmethod(requires=[], version=None):
    # This  allows method  to register class methods inheriting of BasePipe as steps.
    # It basically just step an "is_step" stamp on the method that are defined as steps.
    # This stamp will later be used in the metaclass __new__ to set additionnal usefull attributes to those methods
    def registrate(function : Callable):
        function.requires = [requires] if not isinstance(requires, list) else requires
        function.is_step = True
        function.version = version
        function.step_name = function.__name__
        return function

    return registrate

class BaseStep:

    def __init__(
        self,
        pipeline: "BasePipeline",
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
        self.version = self.worker.version 
        self.requires = self.worker.requires
        self.step_name = self.worker.step_name

        self.worker = MethodType(worker.__func__, self)

        self.make_wrapped_functions()
        
        update_wrapper(self, self.worker)

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

    @property
    def single_step(self):
        return self.pipe.single_step

    def __call__(self, *args, **kwargs):
        return self.worker(*args, **kwargs)

    def __repr__(self):
        return f"<{self.pipe_name}.{self.step_name} StepObject>"

    def is_step_child(self, step_name):
        step = self.pipe.steps[step_name]

        ...

    def object(self):
        #TODO : return current output of the pipe object, with version matching the hierarchy 
        print(self.full_name)
        ...

    def make_wrapped_functions(self):
        self.save = self.make_wrapped_save()
        self.load = self.make_wrapped_load()
        self.generate = self.make_wrapped_generate()

    def make_wrapped_save(self):
        @wraps(self.pipe.disk_class.save)
        def wrapper(session, data, extra=""):
            self.pipeline.resolve()
            disk_object = self.pipe.disk_class(session, self, extra=extra)
            disk_object.check_disk()
            return disk_object.save(data)

        return self.pipe.dispatcher(wrapper)

    def make_wrapped_load(self):
        @wraps(self.pipe.disk_class.load)
        def wrapper(session, extra=""):
            self.pipeline.resolve()
            disk_object = self.pipe.disk_class(session, self, extra=extra)
            disk_object.check_disk()
            return disk_object.load()

        return self.pipe.dispatcher(wrapper)

    def make_wrapped_generate(self):
        return self.pipe.dispatcher(loggedmethod(self.generation_mechanism))

    def get_level(self, selfish = False):
        self.pipeline.resolve()
        return StepLevel(self).resolve_level(selfish = selfish)

    @property
    def generation_mechanism(self):
        @wraps(self.worker)
        def wrapper(
            session,
            *args,
            extra="",
            skip=False,
            refresh=False,
            run_requirements=False,
            save_output=True,
            **kwargs,
        ):
            logger = logging.getLogger("generation")
            self.pipeline.resolve()

            if refresh and skip:
                raise ValueError(
                    """You tried to set refresh (or refresh_main_only) to True and skipping to True simultaneouly. 
                    Stopped code to prevent mistakes : You probably set this by error as both have antagonistic effects. 
                    (skipping passes without loading if file exists, refresh overwrites after generating output if file exists) 
                    Please change arguments according to your clarified intention."""
                )

            disk_object = self.pipe.disk_class(session, self, extra=extra)

            if not refresh:
                if disk_object.is_loadable() :
                    if skip :
                        logger.info(
                            f"File exists for {self.pipe_name}{'.' + extra if extra else ''}. Loading and processing have been skipped"
                        )
                        return None
                
                    logger.debug(f"Found data. Trying to load it in generation context")
                
                    try:
                        result = disk_object.load()
                    except IOError as e:
                        raise IOError(
                            "The DiskObject returned True to `is_loadable()` but the loading procedure failed. Double check and test your DiskObject check_disk and load implementation"
                        ) from e

                    logger.info(
                        f"Loaded {self.pipe_name}{'.' + extra if extra else ''} file. Processing has been skipped "
                    )
                    return result
                else:
                    if disk_object.version_deprecated() or disk_object.step_missing_requirements():
                        logger.info(
                            f"File(s) have been found but with a step too low in the requirement stack or with a wrong version identifier."
                        )
                    else :
                        logger.info(
                            f"Could not find or load {self.pipe_name}{'.' + extra if extra else ''} saved file."
                        )
            else :
                logger.info(
                    f"`refresh` was set to True, ignoring the state of disk files and running the function."
                )

            if run_requirements :
                for step in self.requires :
                    logger.info(
                        f"Running requirement {step.full_name}"
                    )
                    # check via generate **direct** steps recursively before continuing with current step run. 
                    # If they are present, recursive checks will not be run
                    step.generate(session, skip = True, run_requirements = True) 
                    

            logger.info(
                f"Performing the computation to generate {self.pipe_name}{'.' + extra if extra else ''}. Hold tight."
            )
            result = self.pipe.pre_run_wrapper(self.worker(session, *args, extra=extra, **kwargs))

            if save_output:
                logger.info(
                    f"Saving the generated output : {self.pipe_name}{'.' + extra if extra else ''}."
                )
                disk_object.save(result)
            return result
        return wrapper
    
@dataclass
class StepLevel:
    level : int = None

    def __init__(self, step):

        self.requires = self.instanciate(step.requires)
        self.pipe_name = step.pipe_name

    def instanciate(self,requirements):
        
        new_req = []
        for req in requirements : 
            req = StepLevel(req)
            new_req.append(req)
        return new_req

    def resolve_level(self, selfish = False):

        if selfish != False and selfish == True :
            selfish = self

        if self.level is not None :
            return self.level
        
        if len(self.requires) == 0:
            self.level = 0
            return self.level
        
        levels = []
        for req in self.requires :
            if selfish != False and selfish.pipe_name != req.pipe_name :
                continue
            levels.append(req.resolve_level(selfish))

        if len(levels) == 0:
            self.level = 0
            return self.level
        
        self.level = max(levels) + 1
        return self.level