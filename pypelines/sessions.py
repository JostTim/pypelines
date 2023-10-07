import pandas as pd, os

@pd.api.extensions.register_series_accessor("pipeline")
class SeriesPipelineAcessor:
    def __init__(self, pandas_obj) -> None:
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        required_fields = ["path", "subject", "date", "number"]
        missing_fields = []
        for req_field in required_fields:
            if not req_field in obj.index:
                missing_fields.append(req_field)
        if len(missing_fields):
            raise AttributeError(
                f"The series must have some fields to use one acessor. This object is missing fields : {','.join(missing_fields)}"
            )
        
    def subject(self):
        return str(self._obj.subject)

    def number(self, zfill = 3):
        number = str(self._obj.number) if self._obj.number is not None else ""
        number = (
            number
            if zfill is None or number == ""
            else number.zfill(zfill)
        )
        return number

    def alias(self, separator = "_" , zfill = 3 , date_format = None):

        subject = self.subject()
        date = self.date(date_format)
        number = self.number(zfill)

        return (
            subject
            + separator
            + date
            + ((separator + number) if number else "")
        )

    def date(self, format = None):
        if format :
            return self._obj.date.strftime(format)
        return str(self._obj.date)

@pd.api.extensions.register_dataframe_accessor("pipeline")
class DataFramePipelineAcessor:
    def __init__(self, pandas_obj) -> None:
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        required_columns = ["path", "subject", "date", "number"]
        missing_columns = []
        for req_col in required_columns:
            if not req_col in obj.columns:
                missing_columns.append(req_col)
        if len(missing_columns):
            raise AttributeError(
                f"The series must have some fields to use one acessor. This object is missing fields : {','.join(missing_columns)}"
            )

class Session(pd.Series):
    def __new__(
        cls,
        series=None,
        *,
        subject=None,
        date=None,
        number=None,
        path=None,
        auto_path=False,
        date_format = None,
        zfill = 3,
        separator = "_"
    ):
        if series is None:
            series = pd.Series()

        if subject is not None:
            series["subject"] = subject
        if date is not None:
            series["date"] = date
        if number is not None or "number" not in series.index:
            series["number"] = number
        if path is not None:
            series["path"] = path

        series.pipeline  # verify the series complies with pipeline acessor

        if auto_path:
            series["path"] = os.path.normpath(os.path.join(
                series["path"],
                series.pipeline.subject(),
                series.pipeline.date(date_format),
                series.pipeline.number(zfill)
            ))

        if series.name is None:
            series.name = series.pipeline.alias(separator = separator, zfill = zfill , date_format = date_format)

        if not "alias" in series.index:
            series["alias"] = series.pipeline.alias(separator = separator, zfill = zfill , date_format = date_format)

        return series

class Sessions(pd.DataFrame):
    def __new__(cls, series_list):
        # also works seamlessly if a dataframe is passed and is already a Sessions dataframe.
        df = pd.DataFrame(series_list)

        df.pipeline  # verify the df complies with pipeline acessor, then returns

        return df