import pandas as pd
from ..pipelines import Pipeline

from .typing import SessionPipelineAccessorProto

# This is only for type checkers, has no runtime effect
pd.DataFrame.pypeline: SessionPipelineAccessorProto


def extend_pandas():
    if not hasattr(pd.DataFrame, "_pypelines_accessor_registered"):

        @pd.api.extensions.register_dataframe_accessor("pypeline")
        class SessionPipelineAccessor:
            def __init__(self, pandas_obj: pd.DataFrame):
                self._obj = pandas_obj

            def __call__(self, pipeline: Pipeline):
                self.pipeline = pipeline
                return self

            def output_exists(self, pipe_step_name: str):
                names = pipe_step_name.split(".")
                if len(names) == 1:
                    pipe_name = names[0]
                    step_name = self.pipeline.pipes[pipe_name].ordered_steps("highest")[0].step_name
                elif len(names) == 2:
                    pipe_name = names[0]
                    step_name = names[1]
                else:
                    raise ValueError("pipe_step_name should be either a pipe_name.step_name or pipe_name")
                complete_name = f"{pipe_name}.{step_name}"
                return self._obj.apply(
                    lambda session: self.pipeline.pipes[pipe_name]
                    .steps[step_name]
                    .get_disk_object(session)
                    .is_loadable(),
                    axis=1,
                ).rename(complete_name)

            def add_ouput(self, pipe_step_name: str):
                return self._obj.assign(**{pipe_step_name: self.output_exists(pipe_step_name)})

            def where_output(self, pipe_step_name: str, exists: bool):
                new_obj = SessionPipelineAccessor(self._obj)(self.pipeline).add_ouput(pipe_step_name)
                return new_obj[new_obj[pipe_step_name] == exists]
