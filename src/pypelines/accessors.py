from pandas.api.extensions import register_series_accessor, register_dataframe_accessor
import pandas as pd

try:
    # delete the accessor to avoid warning
    del pd.Series.pipeline
except AttributeError:
    pass


@register_series_accessor("pipeline")
class SeriesPipelineAcessor:
    def __init__(self, pandas_obj) -> None:
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
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
        return str(self._obj.subject)

    def number(self, zfill=3):
        number = str(self._obj.number) if self._obj.number is not None else ""
        number = number if zfill is None or number == "" else number.zfill(zfill)
        return number

    def alias(self, separator="_", zfill=3, date_format=None):
        subject = self.subject()
        date = self.date(date_format)
        number = self.number(zfill)

        return subject + separator + date + ((separator + number) if number else "")

    def date(self, format=None):
        if format:
            return self._obj.date.strftime(format)
        return str(self._obj.date)


try:
    # delete the accessor to avoid warning
    del pd.DataFrame.pipeline
except AttributeError:
    pass


@register_dataframe_accessor("pipeline")
class DataFramePipelineAcessor:
    def __init__(self, pandas_obj) -> None:
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        required_columns = ["path", "subject", "date", "number"]
        missing_columns = []
        for req_col in required_columns:
            if req_col not in obj.columns:
                missing_columns.append(req_col)
        if len(missing_columns):
            raise AttributeError(
                "The series must have some fields to use one acessor. This object is missing fields :"
                f" {','.join(missing_columns)}"
            )
