from typing import Protocol
import pandas as pd
from ..pipelines import Pipeline


class SessionPipelineAccessorProto(Protocol):
    def __call__(self, pipeline: Pipeline) -> "SessionPipelineAccessorProto": ...
    def output_exists(self, pipe_step_name: str) -> pd.Series: ...
