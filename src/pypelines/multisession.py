import pandas as pd

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .steps import BaseStep


class BaseMultisessionAccessor:
    step: "BaseStep"

    def __init__(self, parent):
        """Initializes a new instance of the class.

        Args:
            parent: The parent object.

        Attributes:
            step: The parent object.
            _packer: The multisession packer object from the parent's disk class.
            _unpacker: The multisession unpacker object from the parent's disk class.
        """
        self.step = parent
        self._packer = self.step.pipe.disk_class.multisession_packer
        self._unpacker = self.step.pipe.disk_class.multisession_unpacker

    def load(self, sessions, extras=None):
        """Load sessions with optional extras and return packed result.

        Args:
            sessions (DataFrame): The sessions to load.
            extras (list or tuple, optional): Extra values to be used during loading. If not provided,
                the same extra value will be used for all sessions.

        Returns:
            dict: A dictionary containing the loaded sessions.

        Raises:
            ValueError: If the number of extra values supplied is different than the number of sessions.
        """
        session_result_dict = {}

        if not isinstance(extras, (list, tuple)):
            extras = [extras] * len(sessions)

        if len(extras) != len(sessions):
            raise ValueError(
                "The number of extra values supplied is different than the number of sessions. Cannot map them."
            )

        for (index, session), extra in zip(sessions.iterrows(), extras):
            session_result_dict[index] = self.step.load(session, extra=extra)

        return self._packer(sessions, session_result_dict)

    def save(self, sessions, datas, extras=None):
        """Save the data for each session with optional extra values.

        Args:
            sessions (list): List of session objects.
            datas (list): List of data objects corresponding to each session.
            extras (list, tuple, optional): List of extra values to be saved along with each session's data.
                If not provided, the same extra value will be used for all sessions.

        Raises:
            ValueError: If the number of extra values supplied is different than the number of sessions.

        Returns:
            None
        """
        if not isinstance(extras, (list, tuple)):
            extras = [extras] * len(sessions)

        if len(extras) != len(sessions):
            raise ValueError(
                "The number of extra values supplied is different than the number of sessions. Cannot map them."
            )

        for (session, data), extra in zip(self._unpacker(sessions, datas), extras):
            self.step.save(session, data, extra=extra)

        return None

    def generate(self, sessions, *args, extras=None, extra=None, **kwargs):
        """Generate session results based on provided extras for each session.

        Args:
            sessions (pandas.DataFrame): The sessions data to generate results for.
            *args: Additional positional arguments to pass to the generation step.
            extras (list or None): List of extra values to be used for each session.
                If None, the same extra value will be used for all sessions.
            extra: Deprecated argument. Use extras instead.
            **kwargs: Additional keyword arguments to pass to the generation step.

        Returns:
            dict: A dictionary containing the generated results for each session.
        """
        session_result_dict = {}

        if extra is not None:
            if extras is not None:
                raise ValueError(
                    "Ambiguous arguments extra and extras. "
                    "They cannot be used at the same time.\n"
                    "extra sets the same extra value for all session, "
                    "while extras excepts a list like and "
                    "allows to set one value per session (order based)"
                )
            extras = [extra] * len(sessions)
        else:
            if extras is None:
                extras = [extras] * len(sessions)
        if not isinstance(extras, (list, tuple)):
            raise ValueError(
                "if extras is used, it must be a list of the same size of the sessions number supplied. "
                "If you want to supply a single value for all, use extra instead of extras"
            )

        if len(extras) != len(sessions):
            raise ValueError(
                "The number of extra values supplied is different than the number of sessions. Cannot map them."
            )

        for (index, session), extra in zip(sessions.iterrows(), extras):
            session_result_dict[index] = self.step.generate(session, *args, extra=extra, **kwargs)

        return self._packer(sessions, session_result_dict)

    def start_tasks(self, sessions):
        """Starts tasks for each session in the given sessions.

        Args:
            sessions: A pandas DataFrame containing sessions.

        Returns:
            None
        """
        for session in sessions.iterrows():
            self.step.task.start(session)


def assert_dataframe(sessions):
    """Check if the input is a pandas DataFrame.

    Args:
        sessions (pd.DataFrame): The input to be checked.

    Returns:
        bool: True if the input is a pandas DataFrame.

    Raises:
        ValueError: If the input is a pandas Series or not a DataFrame.
    """
    if isinstance(sessions, pd.DataFrame):
        return True
    elif isinstance(sessions, pd.Series):
        raise ValueError("sessions argument appears to be a pandas series. Did you forgot to use a dataframe instead ?")
    else:
        raise ValueError(f"sessions argument must be a dataframe. It appears to be of type {type(sessions).__name__}")
