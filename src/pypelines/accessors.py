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


try:
    # delete the accessor to avoid warning
    del pd.DataFrame.pipeline
except AttributeError:
    pass


@register_dataframe_accessor("pipeline")
class DataFramePipelineAcessor:
    def __init__(self, pandas_obj) -> None:
        """Initialize the object with a pandas DataFrame or Series.

        Args:
            pandas_obj: A pandas DataFrame or Series to be validated and stored.

        Returns:
            None
        """
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        """Validate if the input object has all the required columns.

        Args:
            obj: pandas DataFrame or Series. The object to be validated.

        Raises:
            AttributeError: If the input object is missing any of the required columns.
        """
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
