from functools import wraps, partial, update_wrapper

def step(requires = []):
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
        self.load = self._loading_wrapper(self.pipe.file_loader)
        self.save = self._saving_wrapper(self.pipe.file_saver)
        self.generate = self._generating_wrapper(self.step)

        update_wrapper(self, self.step)

    def __call__(self, *args, **kwargs):
        return self.step(*args, **kwargs)

    def __repr__(self):
        return f"<{self.pipe_name}.{self.step_name} StepObject>"

    def _saving_wrapper(self, function):
        return self.pipe.dispatcher(self._version_wrapper(function, self.pipe.step_version))

    def _loading_wrapper(self, function):
        return self.pipe.dispatcher(self._version_wrapper(function, self.pipe.step_version))

    def _generating_wrapper(self, function):
        return self.pipe.dispatcher(
            session_log_decorator
        )

    def _version_wrapper(self, function_to_wrap, version_getter):
        @wraps(function_to_wrap)
        def wrapper(*args,**kwargs):
            version = version_getter(self)
            return function_to_wrap(*args, version=version, **kwargs)
        return wrapper
