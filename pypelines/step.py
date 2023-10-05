from functools import wraps, partial, update_wrapper
from .loggs import loggedmethod
import logging
from typing import Callable

def stepmethod(requires = []):
    # This method allows to register class methods inheriting of BasePipe as steps.
    # It basically just step an "is_step" stamp on the method that are defined as steps.
    # This stamp will later be used in the metaclass __new__ to set additionnal usefull attributes to those methods
    if not isinstance(requires, list):
        requires = [requires]
        
    def registrate(function):
        function.requires = requires
        function.is_step = True
        return function
    return registrate

class BaseStep:
    
    def __init__(self, pipeline, pipe, step, step_name):
        self.pipeline = pipeline # save an instanciated access to the pipeline parent
        self.pipe = pipe # save an instanciated access to the pipe parent
        self.step = step # save an instanciated access to the step function (undecorated)
        self.pipe_name = pipe.pipe_name
        self.step_name = step_name

        self.single_step = self.pipe.single_step
        self.requires = self.step.requires
        self.is_step = True
        
        self.requirement_stack = partial( self.pipeline.get_requirement_stack, instance = self )
        self.step_version = partial( self.pipe.step_version, step = self )

        update_wrapper(self, self.step)
        self._make_wrapped_functions()

    def __call__(self, *args, **kwargs):
        return self.step(*args, **kwargs)

    def __repr__(self):
        return f"<{self.pipe_name}.{self.step_name} StepObject>"

    def _saving_wrapper(self, function):
        return self.pipe.dispatcher(self._version_wrapper(function, self.pipe.step_version))

    def _loading_wrapper(self, function):
        return self.pipe.dispatcher(self._version_wrapper(function, self.pipe.step_version))

    def _generating_wrapper(self, function):
        return 

    def _make_wrapped_functions(self):
        self.make_wrapped_save()
        self.make_wrapped_load()
        self.make_wrapped_generate()

    def make_wrapped_save(self):
        self.save = self._saving_wrapper(self.pipe.file_saver)
    
    def make_wrapped_load(self):
        self.load = self._loading_wrapper(self.pipe.file_loader)
    
    def make_wrapped_generate(self):
        self.generate = loggedmethod(
            self._version_wrapper(
                self.pipe.dispatcher(
                    self._loading_wrapper(
                        self._saving_wrapper(
                            self.pipe.pre_run_wrapper(self.step)
                            )
                        )
                    )
                )
            )

            
    def _version_wrapper(self, function_to_wrap, version_getter):
        @wraps(function_to_wrap)
        def wrapper(*args,**kwargs):
            version = version_getter(self)
            return function_to_wrap(*args, version=version, **kwargs)
        return wrapper

    def _loading_wrapper(self, func: Callable):  
        """
        Decorator to load instead of calculating if not refreshing and saved data exists
        """

        @wraps(func)
        def wrap(session_details, *args, **kwargs):
            """
            Decorator function

            Parameters
            ----------
            *args : TYPE
                DESCRIPTION.
            **kwargs : TYPE
                DESCRIPTION.

            Returns
            -------
            TYPE
                DESCRIPTION.

            """
            logger = logging.getLogger("load_pipeline")

            kwargs = kwargs.copy()
            extra = kwargs.get("extra", None)
            skipping = kwargs.pop("skip", False)
            # we raise if file not found only if skipping is True
            refresh = kwargs.get("refresh", False)
            refresh_main_only = kwargs.get("refresh_main_only", False)

            if refresh_main_only:
                # we set refresh true no matter what and then set
                # refresh_main_only to False so that possible childs functions will never do this again
                refresh = True
                kwargs["refresh"] = False
                kwargs["refresh_main_only"] = False

            if refresh and skipping:
                raise ValueError(
                    """You tried to set refresh (or refresh_main_only) to True and skipping to True simultaneouly. 
                    Stopped code to prevent mistakes : You probably set this by error as both have antagonistic effects. 
                    (skipping passes without loading if file exists, refresh overwrites after generating output if file exists) 
                    Please change arguments according to your clarified intention."""
                )

            if not refresh:
                if skipping and self.pipe.file_checker(session_details, extra):
                    logger.load_info(
                        f"File exists for {self.pipe_name}{'.' + extra if extra else ''}. Loading and processing have been skipped"
                    )
                    return None
                logger.debug(f"Trying to load saved data")
                try:
                    result = self.pipe.file_loader(session_details, extra=extra)
                    logger.load_info(
                        f"Found and loaded {self.pipe_name}{'.' + extra if extra else ''} file. Processing has been skipped "
                    )
                    return result
                except IOError:
                    logger.load_info(
                        f"Could not find or load {self.pipe_name}{'.' + extra if extra else ''} saved file."
                    )

            logger.load_info(
                f"Performing the computation to generate {self.pipe_name}{'.' + extra if extra else ''}. Hold tight."
            )
            return func(session_details, *args, **kwargs)

        return wrap

    def _saving_wrapper(self, func: Callable):
        # decorator to load instead of calculating if not refreshing and saved data exists
        @wraps(func)
        def wrap(session_details, *args, **kwargs):

            logger = logging.getLogger("save_pipeline")

            kwargs = kwargs.copy()
            extra = kwargs.get("extra", "")
            save_pipeline = kwargs.pop("save_pipeline", True)

   
            result = func(session_details, *args, **kwargs)
            if session_details is not None:
                if save_pipeline:
                    # we overwrite inside saver, if file exists and save_pipeline is True
                    self.pipe.file_checker(result, session_details, extra=extra)
            else:
                logger.warning(
                    f"Cannot guess data saving location for {self.pipe_name}: 'session_details' argument must be supplied."
                )
            return result

        return wrap
