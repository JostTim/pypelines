import os, re
from .sessions import Session
import pickle

from typing import Callable, Type, Iterable, Literal, Protocol, TYPE_CHECKING

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
        return f"{self.step.relative_name}{'.' + self.extra if self.extra else ''}"

    @abstractmethod
    def version_deprecated(self) -> bool:
        return False

    @abstractmethod
    def step_level_too_low(self) -> bool:
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
        session_result_dict = {
            sessions.loc[key].u_alias: value for key, value in session_result_dict.items()
        }  # replace indices from session id with session u_alias

        return session_result_dict

    @staticmethod
    def multisession_unpacker(sessions, datas):
        raise NotImplementedError

    def disk_step_instance(self) -> "BaseStep | None":
        """Returns an instance of the step that corresponds to the file on disk."""
        if self.disk_step is not None:
            return self.step.pipe.steps[self.disk_step]
        return None

    def is_matching(self):
        if self.is_loadable() and not (self.version_deprecated() or self.step_level_too_low()):
            return True
        return False

    def is_loadable(self) -> bool:
        return self.loadable

    def get_found_disk_object_description(self) -> str:
        return ""

    def get_status_message(self):
        loadable_disk_message = "A disk object is loadable. " if self.is_loadable() else ""
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
            f"{self.object_name} object has {'a' if self.is_matching() else 'no'} valid disk object found."
            f" {found_disk_object_description}{loadable_disk_message}"
        )


class NullDiskObject(BaseDiskObject):
    def version_deprecated(self) -> bool:
        return True

    def step_level_too_low(self) -> bool:
        return True

    def check_disk(self) -> bool:
        return False

    def save(self, data: OutputData) -> None:
        # data is not saved to disk
        pass

    def load(self) -> OutputData:
        # this should never be called as check_disk always return False
        raise NotImplementedError


_CACHE_STORAGE = {}  # this cache variable is cross instances


class CachedDiskObject(BaseDiskObject):
    def __init__(self, session: Session, step: "BaseStep", extra="") -> None:
        self.session = session
        self.step = step
        self.extra = extra
        self.storage = _CACHE_STORAGE
        self.loadable = self.check_disk()

    def get_cached_storage(self):
        if self.step.pipe not in self.storage:
            self.storage[self.step.pipe] = {}

        if self.session.name not in self.storage[self.step.pipe].keys():
            self.storage[self.step.pipe][self.session.name] = {}

        if str(self.extra) not in self.storage[self.step.pipe][self.session.name].keys():
            stored_dict = self.save(None)
        else:
            stored_dict = self.storage[self.step.pipe][self.session.name][str(self.extra)]

        return stored_dict

    def load(self):
        return self.get_cached_storage()["content"]

    def save(self, data):
        stored_dict = {
            "version": self.step.version,
            "content": data,
            "step": self.step.step_name,
        }
        self.storage[self.step.pipe][self.session.name][str(self.extra)] = stored_dict
        return stored_dict

    def check_disk(self):
        stored_cache = self.get_cached_storage()
        self.disk_version = stored_cache["version"]
        self.disk_step = stored_cache["step"]

        if stored_cache["content"] is None:
            return False

        return True

    def version_deprecated(self):
        if self.step.version != self.disk_version:
            return True
        return False

    def step_level_too_low(self) -> bool:
        # we get the step instance that corresponds to the one on the disk
        disk_step = self.disk_step_instance()

        # we compare levels with the currently called step
        # if disk step level < current called step level, we return True, else we return False.
        if disk_step.get_level(selfish=True) < self.step.get_level(selfish=True):
            return True
        return False

    def clear_cache(self):
        for pipe in list(self.storage.keys()):
            self.storage.pop(pipe)
