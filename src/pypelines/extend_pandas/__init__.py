import pandas as pd
from pandas.api.extensions import register_dataframe_accessor, register_series_accessor
from ..pipelines import Pipeline

from .typing import SessionPipelineAccessorProto

# This is only for type checkers, has no runtime effect
# pd.DataFrame.pypeline: SessionPipelineAccessorProto


def extend_pandas():
    if hasattr(pd.DataFrame, "_accessors") and "pypeline" in getattr(pd.DataFrame, "_accessors"):
        pass
    else:

        @register_dataframe_accessor("pypeline")
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

    if hasattr(pd.Series, "_accessors") and "pypeline" in getattr(pd.Series, "_accessors"):
        pass

    else:

        @register_series_accessor("pypeline")
        class SeriesPipelineAcessor:
            def __init__(self, pandas_obj) -> None:
                """Initializes the class with a pandas object after validating it.

                Args:
                    pandas_obj: A pandas object to be validated and stored.

                Returns:
                    None
                """
                self._validate(pandas_obj)
                self._obj = pandas_obj

            @staticmethod
            def _validate(obj):
                """Validate if the object has all the required fields.

                Args:
                    obj: pandas.Series: The object to be validated.

                Raises:
                    AttributeError: If the object is missing any of the required fields.
                """
                required_fields = ["path", "subject", "date", "number"]
                missing_fields = []
                for req_field in required_fields:
                    if req_field not in obj.index:
                        missing_fields.append(req_field)
                if len(missing_fields):
                    raise AttributeError(
                        "The series must have some fields to use one acessor. This object is missing fields :"
                        f" {','.join(missing_fields)}"
                    )

            def subject(self):
                """Return the subject of the object as a string."""
                return str(self._obj.subject)

            def number(self, zfill=3):
                """Return a string representation of the number attribute of the object,
                optionally zero-filled to a specified length.

                    Args:
                        zfill (int): The length to which the number should be zero-filled. Default is 3.

                    Returns:
                        str: A string representation of the number attribute, zero-filled if specified.
                """
                number = str(self._obj.number) if self._obj.number is not None else ""
                number = number if zfill is None or number == "" else number.zfill(zfill)
                return number

            def alias(self, separator="_", zfill=3, date_format=None):
                """Generate an alias based on the subject, date, and number.

                Args:
                    separator (str): The separator to use between the subject, date, and number. Default is "_".
                    zfill (int): The zero padding for the number. Default is 3.
                    date_format (str): The format of the date. If None, the default format is used.

                Returns:
                    str: The generated alias.
                """
                subject = self.subject()
                date = self.date(date_format)
                number = self.number(zfill)

                return subject + separator + date + ((separator + number) if number else "")

            def date(self, format=None):
                """Return the date in the specified format if provided, otherwise return the date as a string.

                Args:
                    format (str, optional): The format in which the date should be returned. Defaults to None.

                Returns:
                    str: The date in the specified format or as a string.
                """
                if format:
                    return self._obj.date.strftime(format)
                return str(self._obj.date)
