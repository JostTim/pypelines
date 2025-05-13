import os, re
from .sessions import Session
import pickle

from typing import Callable, Type, Iterable, Literal, Protocol, TYPE_CHECKING, List, cast

from abc import ABCMeta, abstractmethod
from functools import wraps

if TYPE_CHECKING:
    from .steps import BaseStep


class OutputData(Protocol):
    """Can be a mapping, iterable, single element, or None.

    This class is defined for typehints, and is not a real class useable at runtime"""


class BaseDiskObject(metaclass=ABCMeta):
    step_traceback: Literal["none", "single", "multi"] = "none"

    disk_version = None
    disk_step = None

    step: "BaseStep"
    session: Session
    extra: str

    def __init__(self, session: Session, step: "BaseStep", extra="") -> None:
        """Initialize the ShortLivedObject with the given session, step, and optional extra data.

        Args:
            session (Session): The session object to use.
            step (BaseStep): The step object to use.
            extra (str, optional): Extra data to include. Defaults to "".

        Returns:
            None

        Notes:
            This object is meant to be short lived. Created, check drive,
            and quickly take action by saving or loading file according to the procedures defined.
            The behavior is not meant to be edited after the init so that's why the methods
            don't take arguments, at the exception of the save method which takes data to save as input.
        """
        # this object is meant to be short lived. Created, check drive,
        # and quickly take action by saving or loading file according to the procedures defined.
        # The behaviour is not meant to be edited after the init so that's why the methods
        # don't take arguments, at the exception of the save method wich takes data to save as input.

        self.session = session
        self.step = step
        self.extra = extra

        self.loadable = self.check_disk()

    @property
    def object_name(self):
        """Return the full name of the object."""
        return f"{self.step.relative_name}{'.' + self.extra if self.extra else ''}"

    @abstractmethod
    def version_deprecated(self) -> bool:
        """Returns a boolean value indicating whether the version is deprecated."""
        return False

    @abstractmethod
    def step_level_too_low(self) -> bool:
        """Check if the step level is too low.

        Returns:
            bool: True if the step level is too low, False otherwise.
        """
        return False

    @abstractmethod
    def check_disk(self) -> bool:
        """sets self.disk_version, self.disk_step and self.loadable
        all necessary elements you may need to else know how to load your file next.
        It returns True if it found something it can load, and False in other case"""
        ...

    @abstractmethod
    def save(self, data: OutputData) -> None:
        """Saves the data given as input. Does not take any info to know where to save the data,
        as it should depend on the info given as input to the __init__ method. Extend the __init__ method if
        you need more info to be able to determine the saving behaviour."""
        ...

    @abstractmethod
    def load(self) -> OutputData:
        """Loads the data that do exist on disk.
        If it misses some information of the check_disk didn't found an expected pattern on disk,
        it should raise IOError"""
        ...

    # @staticmethod
    # def multisession_packer(sessions, session_result_dict):
    #     raise NotImplementedError

    @staticmethod
    def multisession_packer(sessions, session_result_dict: dict) -> dict:
        """Packs the results of multiple sessions into a dictionary with session u_alias as keys.

        Args:
            sessions: DataFrame containing session information.
            session_result_dict: Dictionary containing session results with session id as keys.

        Returns:
            Dictionary with session u_alias as keys and corresponding results as values.
        """
        session_result_dict = {
            sessions.loc[key].u_alias: value for key, value in session_result_dict.items()
        }  # replace indices from session id with session u_alias

        return session_result_dict

    @staticmethod
    def multisession_unpacker(sessions, datas):
        """Unpacks data from multiple sessions, to store them to "disk".

        Args:
            sessions (list): A list of session identifiers.
            datas (list): A list of data corresponding to each session.

        Raises:
            NotImplementedError: This function is not implemented yet.
        """
        raise NotImplementedError

    def disk_step_instance(self) -> "BaseStep | None":
        """Returns an instance of the step that corresponds to the file on disk."""
        from .steps import BaseStep

        if self.disk_step is not None:
            if isinstance(self.disk_step, BaseStep):
                return self.disk_step
            elif isinstance(self.disk_step, str):
                return self.step.pipe.steps[self.disk_step]
            raise TypeError(f"Type must be BaseStep or str, found {type(self.disk_step)}")
        return None

    def is_matching(self):
        """Check if the object is matching the required criteria.

        Returns:
            bool: True if the object is matching, False otherwise.
        """
        if self.is_loadable() and not (self.version_deprecated() or self.step_level_too_low()):
            return True
        return False

    def is_loadable(self) -> bool:
        """Check if the object is loadable.

        Returns:
            bool: True if the object is loadable, False otherwise.
        """
        return self.loadable

    def get_found_disk_object_description(self) -> str:
        """Return the description of the found disk object.

        Returns:
            str: The description of the found disk object.
        """
        return ""

    def get_status_message(self):
        """Return a status message for the object.

        Returns:
            str: A message describing the status of the object, including loadability, deprecation, step level,
                and found disk object description.
        """

        session = self.session.alias
        step = self.step.complete_name
        extra = str(self.extra) if self.extra is not None else ""

        loadable_disk_message = "Disk object for A disk object is loadable. " if self.is_loadable() else ""
        deprecated_disk_message = (
            f"This object's version is {'deprecated' if self.version_deprecated() else 'the current one'}. "
        )
        step_level_disk_message = (
            "This object's step level is"
            f" {'too low' if self.step_level_too_low() else f'at least equal or above the {self.step.step_name} step'}"
        )

        loadable_disk_message = (
            loadable_disk_message + deprecated_disk_message + step_level_disk_message
            if loadable_disk_message
            else loadable_disk_message
        )

        found_disk_object_description = (
            "The disk object found is : " + self.get_found_disk_object_description() + ". "
            if self.get_found_disk_object_description()
            else ""
        )
        return (
            f"{self.object_name} object has {'a' if self.is_matching() else 'no'} valid disk object found.\n"
            f" {found_disk_object_description}{loadable_disk_message}"
        )


class NullDiskObject(BaseDiskObject):
    """Class representing a Null Disk Object, which simulates a disk object with methods that always indicate
    version deprecation and False as a check disk status, but does not perform actual disk operations to check that.
    It will allways trigger a run, when calling generate on the step that use it.
    """

    def version_deprecated(self) -> bool:
        """Indicates that the version of the function is deprecated.

        Returns:
            bool: True if the version is deprecated, False otherwise.
        """
        return True

    def step_level_too_low(self) -> bool:
        """Check if the step level is too low.

        Returns:
            bool: True if the step level is too low, False otherwise.
        """
        return True

    def check_disk(self) -> bool:
        """Check the disk status.

        Returns:
            bool: True if disk is healthy, False otherwise.
        """
        return False

    def save(self, data: OutputData) -> None:
        """Save the output data to disk.

        Args:
            data (OutputData): The output data to be saved.

        Returns:
            None
        """
        # data is not saved to disk
        pass

    def load(self) -> OutputData:
        """Load the output data.

        Returns:
            OutputData: The output data object.

        Raises:
            NotImplementedError: This should never be called as check_disk always returns False.
        """
        # this should never be called as check_disk always return False
        raise NotImplementedError


_CACHE_STORAGE = {}  # this cache variable is cross instances


class CachedDiskObject(BaseDiskObject):
    def __init__(self, session: Session, step: "BaseStep", extra="") -> None:
        """Initialize the BaseStepLoader.

        Args:
            session (Session): The session object.
            step (BaseStep): The BaseStep object.
            extra (str, optional): Extra information. Defaults to "".

        Returns:
            None
        """
        self.session = session
        self.step = step
        self.extra = extra
        self.storage = _CACHE_STORAGE
        self.loadable = self.check_disk()

    def get_cached_storage(self):
        """Return cached storage for the current step, session, and extra data.

        Returns:
            dict: A dictionary containing the cached storage for the current step, session, and extra data.
        """
        step_dedicated_storage = self.storage.setdefault(self.step.complete_name, {}).setdefault(self.session.name, {})
        dedicated_key = f"extra#{self.extra}"
        if dedicated_key not in step_dedicated_storage.keys():
            return self.wrap_up_data(None)
        return step_dedicated_storage[dedicated_key]

    def load(self):
        """Load the content from the cached storage."""
        return self.get_cached_storage()["content"]

    def wrap_up_data(self, data):
        stored_dict = {
            "version": self.step.version,
            "content": data,
            "step": self.step.step_name,
        }
        return stored_dict

    def save(self, data):
        """Save the data into the storage dictionary.

        Args:
            data: The data to be saved.

        Returns:
            dict: A dictionary containing the version, content, and step name of the saved data.
        """
        step_dedicated_storage = self.storage.setdefault(self.step.complete_name, {}).setdefault(self.session.name, {})
        dedicated_key = f"extra#{self.extra}"
        stored_dict = self.wrap_up_data(data)
        step_dedicated_storage[dedicated_key] = stored_dict
        return stored_dict

    def check_disk(self):
        """Check the disk status and return True if the disk content is not None, otherwise return False."""
        stored_cache = self.get_cached_storage()
        self.disk_version = stored_cache["version"]
        self.disk_step = stored_cache["step"]

        if stored_cache["content"] is None:
            return False

        return True

    def version_deprecated(self):
        """Check if the version is deprecated.

        Returns:
            bool: True if the version is deprecated, False otherwise.
        """
        if self.step.version != self.disk_version:
            return True
        return False

    def step_level_too_low(self) -> bool:
        """Check if the level of the disk step is lower than the current step.

        Returns:
            bool: True if the level of the disk step is lower than the current step, False otherwise.
        """
        # we get the step instance that corresponds to the one on the disk
        disk_step = self.disk_step_instance()

        # we compare levels with the currently called step
        # if disk step level < current called step level, we return True, else we return False.
        if disk_step < self.step:
            return True
        return False

    def clear_cache(self):
        """Clears the cache by removing all items stored in the cache."""
        for pipe in list(self.storage.keys()):
            self.storage.pop(pipe)


class FlaggedDiskObject(BaseDiskObject):
    """A disk object that doesn't serve to actually load thinks, but as an indicator that they are avilable elsewhere,
    by indicating with a flag, when the generate / save methods will have been executed.
    If the flag file exists, the load method will be allowed to trigger (returning None) and keep the flow of
    the pipeline running, without executing the runner for the flagged step and the ones below.
    """

    collection = ["preprocessing_saves"]
    file_prefix: str
    extension = "flag"
    flaggable_steps: str | List[str] = "##highest"
    supports_version = False

    def parse_extra(self):
        return f".{self.extra}" if self.extra else ""

    def get_file_name(self, step: "BaseStep"):
        file_prefix = self.file_prefix if hasattr(self, "file_prefix") else "runned"
        version_str = f".{self.step.version}" if self.supports_version else ""
        return (
            f"{file_prefix}.{self.step.pipe_name}."
            f"{step.step_name}{self.parse_extra()}{version_str}"
            f".{self.extension}"
        )

    def get_flag_path(self, step: "BaseStep"):
        return os.path.join(self.session.path, os.path.sep.join(self.collection), self.get_file_name(step))

    def get_flaggable_steps(self) -> "List[BaseStep]":

        def internal_getter():
            if isinstance(self.flaggable_steps, str):
                if self.flaggable_steps.startswith("##"):
                    which = self.flaggable_steps.lstrip("#")
                    if which not in ["highest", "lowest"]:
                        raise ValueError("Must be 'highest' or 'lowest'")
                    return [self.step.pipe.ordered_steps(first=cast(Literal["highest", "lowest"], which))[0]]
                return [self.step.pipe.steps[self.flaggable_steps]]
            return sorted(
                [self.step.pipe.steps[step_name] for step_name in self.flaggable_steps],
                key=lambda step: step.get_level(selfish=True),
                reverse=True,
            )

        if not hasattr(self, "_flaggable_steps"):
            self._flaggable_steps = internal_getter()
        return self._flaggable_steps

    def save(self, data):
        if self.step_supports_flagging():
            flagpath = self.get_flag_path(self.step)
            with open(flagpath, "w"):
                return

    def check_disk(self):
        for flagged_step in self.get_flaggable_steps():
            if flagged_step >= self.step:
                if os.path.isfile(self.get_flag_path(flagged_step)):
                    self.disk_step = flagged_step
                    self.disk_version = flagged_step.version
                    return True
        return False

    def step_supports_flagging(self):
        return self.step in self.get_flaggable_steps()

    def load(self):
        return f"FLAG FOR : {self.get_file_name(self.step)}"

    def step_level_too_low(self) -> bool:
        """Check if the level of the disk step is lower than the current step.

        Returns:
            bool: True if the level of the disk step is lower than the current step, False otherwise.
        """
        # we get the step instance that corresponds to the one on the disk
        disk_step = self.disk_step_instance()

        # we compare levels with the currently called step
        # if disk step level < current called step level, we return True, else we return False.
        if disk_step < self.step:
            return True
        return False

    def version_deprecated(self):
        """Doesn't support versionning yet. Returning always False indicating non deprecation."""
        return False


class CachedFlaggedDiskObject(CachedDiskObject, FlaggedDiskObject):
    """
    Behaves like a CachedDiskObject, but also supports flagging.
    - If cache is available, loads from cache (priority).
    - If not, but a flag is found for a step >= current, loads flag (skips running).
    - If neither, triggers computation.
    """

    def check_disk(self):
        self.cache_found = False
        # 1. Check cache first (priority)
        exists = CachedDiskObject.check_disk(self)
        self.cache_found = exists

        # 2. If not in cache, check for flag
        if not exists:
            exists = FlaggedDiskObject.check_disk(self)

        return exists

    def load(self):
        # If cache is available, load from cache
        if self.cache_found:
            return self.get_cached_storage()["content"]

        # If flag is found, return flag info (or None, or raise, as desired)
        return FlaggedDiskObject.load(self)

    def save(self, data=None):
        CachedDiskObject.save(self, data)
        FlaggedDiskObject.save(self, data)

    def version_deprecated(self):
        # If loaded from cache, check version
        if self.cache_found:
            return CachedDiskObject.version_deprecated(self)
        # If loaded from flag, treat as not deprecated (or implement logic as needed)
        return FlaggedDiskObject.version_deprecated(self)

    def step_level_too_low(self) -> bool:
        # If loaded from cache, use cache logic
        if self.cache_found:
            return CachedDiskObject.step_level_too_low(self)
        # If loaded from flag, use flag logic
        return FlaggedDiskObject.step_level_too_low(self)

    def get_found_disk_object_description(self) -> str:
        if self.cache_found:
            return f"Cache for with step name {self.disk_step}"
        # if flag is still found
        if self.loadable:
            return f"Flag found with step name {self.disk_step.step_name}"
        return f"Cache nor Flag found for step {self.step.step_name}"
