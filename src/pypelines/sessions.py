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
        """Create a new series with specified attributes.

        Args:
            series (pd.Series, optional): The series to be modified. Defaults to None.
            subject (str, optional): The subject to be added to the series. Defaults to None.
            date (str, optional): The date to be added to the series. Defaults to None.
            number (str, optional): The number to be added to the series. Defaults to None.
            path (str, optional): The path to be added to the series. Defaults to None.
            auto_path (bool, optional): Whether to automatically generate the path. Defaults to False.
            date_format (str, optional): The format of the date. Defaults to None.
            zfill (int, optional): The zero-fill width for number formatting. Defaults to 3.
            separator (str, optional): The separator for alias generation. Defaults to "_".

        Returns:
            pd.Series: The modified series with the specified attributes.
        """
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
        """Create a new Sessions dataframe from a list of series.

        Args:
            series_list (list): A list of series to create the Sessions dataframe from.

        Returns:
            pd.DataFrame: A new Sessions dataframe created from the provided series list.

        Raises:
            AttributeError: If the dataframe does not comply with the pipeline accessor.
        """
        # also works seamlessly if a dataframe is passed and is already a Sessions dataframe.
        df = pd.DataFrame(series_list)

        df.pipeline  # verify the df complies with pipeline acessor, then returns

        return df
