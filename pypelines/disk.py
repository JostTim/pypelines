import os, re
from . sessions import Session
import pickle

from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING

from abc import ABCMeta, abstractmethod

if TYPE_CHECKING:
    from .steps import BaseStep

class OutputData(Protocol):
    """Can be a mapping, iterable, single element, or None.

    This class is defined for typehints, and is not a real class useable at runtime"""

class BaseDiskObject(metaclass=ABCMeta) :

    disk_version = None
    disk_step = None

    def __init__(self, session : Session, step : "BaseStep", extra = "") -> None :
        # this object is meant to be short lived. Created, check drive, 
        # and quickly take action by saving or loading file according to the procedures defined.
        # The behaviour is not meant to be edited after the init so that's why the methods 
        # don't take arguments, at the exception of the save method wich takes data to save as input.

        self.session = session
        self.step = step
        self.extra = extra

        self.loadable = self.check_disk()

    @abstractmethod
    def version_deprecated(self) -> bool :
        return False
    
    @abstractmethod
    def step_missing_requirements(self) -> bool :
        return False

    @abstractmethod
    def check_disk(self) -> bool:
        """sets self.disk_version, self.disk_step and self.loadable
        all necessary elements you may need to else know how to load your file next. 
        It returns True if it found something it can load, and False in other case"""
        ...

    @abstractmethod
    def save(self, data : OutputData) -> None:
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

    def is_loadable(self) -> bool:
        return self.loadable

    def get_disk_digest(self):
        return {"step_name":self.disk_step, "version":self.disk_version}
