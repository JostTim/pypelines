from . import accessors
import pandas as pd, os


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
        date_format=None,
        zfill=3,
        separator="_",
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
            series["path"] = os.path.normpath(
                os.path.join(
                    series["path"],
                    series.pipeline.subject(),
                    series.pipeline.date(date_format),
                    series.pipeline.number(zfill),
                )
            )

        if series.name is None:
            series.name = series.pipeline.alias(separator=separator, zfill=zfill, date_format=date_format)

        if "alias" not in series.index:
            series["alias"] = series.pipeline.alias(separator=separator, zfill=zfill, date_format=date_format)

        return series


class Sessions(pd.DataFrame):
    def __new__(cls, series_list):
        # also works seamlessly if a dataframe is passed and is already a Sessions dataframe.
        df = pd.DataFrame(series_list)

        df.pipeline  # verify the df complies with pipeline acessor, then returns

        return df
