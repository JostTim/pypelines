
from .pickle_backend import PicklePipe
from .pipeline import BasePipeline
from .step import stepmethod

class ExamplePipeline(BasePipeline):
    ...

example_pipeline = ExamplePipeline()

@example_pipeline.register_pipe
class ExamplePipe(PicklePipe):

    @stepmethod(version = "1")
    def example_step1(self, session, argument1, optionnal_argument2 = "23"):
        return {"argument1" : argument1, "optionnal_argument2" : optionnal_argument2}

    @stepmethod(requires = "ExamplePipe.example_step1")
    def example_step2(self, session, argument1, argument2):
        return {"argument1" : argument1, "argument2" : argument2}