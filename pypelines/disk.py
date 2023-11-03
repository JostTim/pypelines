import os, re
from .sessions import Session
import pickle

from typing import Callable, Type, Iterable, Literal, Protocol, TYPE_CHECKING

from abc import ABCMeta, abstractmethod

if TYPE_CHECKING:
    from .steps import BaseStep

class OutputData(Protocol):
    """Can be a mapping, iterable, single element, or None.

    This class is defined for typehints, and is not a real class useable at runtime"""
    
class BaseDiskObject(metaclass=ABCMeta):
    step_traceback: Literal["none", "single", "multi"] = "none"

    disk_version = None
    disk_step = None

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
        return f"{self.step.full_name}{'.'+self.extra if self.extra else ''}"

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

    def disk_step_instance(self) -> "BaseStep":
        """Returns an instance of the step that corresponds to the file on disk."""
        if self.disk_step is not None :
            return self.step.pipe.steps[self.disk_step]
        return None

    def is_matching(self):
        if (
            self.is_loadable()
            and not self.version_deprecated()
            and not self.step_level_too_low()
        ):
            return True
        return False

    def is_loadable(self) -> bool:
        return self.loadable
    
    def get_found_disk_object_description(self) -> str :
        return ""
 
    def get_status_message(self):
        loadable_disk_message = "A disk object is loadable. " if self.is_loadable() else ""
        deprecated_disk_message = f"This object's version is { 'deprecated' if self.version_deprecated() else 'the current one' }. "
        step_level_disk_message = f"This object's step level is { 'too low' if self.step_level_too_low() else f'at least equal or above the {self.step.step_name} step' }. "
        
        loadable_disk_message = loadable_disk_message + deprecated_disk_message + step_level_disk_message if loadable_disk_message else loadable_disk_message
        
        found_disk_object_description = "The disk object found is : " + self.get_found_disk_object_description() + ". " if self.get_found_disk_object_description() else ""
        return f"{self.object_name} object has{ ' a' if self.is_matching() else ' no' } valid disk object found. {found_disk_object_description}{loadable_disk_message}"
