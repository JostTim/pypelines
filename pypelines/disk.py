import os
from . sessions import Session

from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .step import BaseStep

class OutputData(Protocol):
    """Can be a mapping, iterable, single element, or None.

    This class is defined for typehints, and is not a real class useable at runtime"""

class BaseDiskObject :

    disk_version = None
    disk_step = None

    def __init__(self, session : Session, step : BaseStep, extra = "") -> None :

        self.step = None
        self.session = session
        self.step = step
        self.extra = extra

        self.check_disk()

    def check_disk(self):
        """sets self.disk_version and self.disk_step"""
        ...

    def save(self, object):
        ...

    def load(self) -> OutputData:
        ...

    def step_exist(self, session : Session):
        """returns True if the file(s) found had a stamp corresponding to the current step. False otherwise"""
        return self.step == self.disk_step

    def version_exist(self, session : Session):
        """returns True if the file found had a stamp for that step corresponding to the current version. False otherwise""" 
        return self.step.version == self.disk_version
    

class PickleObject(BaseDiskObject) :

    collection = ["preprocessing_saves"] #collection a.k.a subfolders in the session.path before the file itself
    file_prefix = "preproc_data"
    extension = "pickle"
    current_suffixes = ""

    def make_file_prefix_path(self):
        prefix_path = self.file_prefix + "." + self.step.pipe_name
        rigid_pattern = self.file_prefix

        pattern = ""

        if self.step.pipe.single_step :
            pass

        if self.step.use_version :
            pass


        flexible_pattern = self.f

    def check_disk(self):
        search_path = os.path.join(self.session.path, self.collection)
        

    def save(self, object):
        ...

    def load(self) -> OutputData:
        ...


import natsort
from . import extract

def files(input_path, re_pattern = None, relative = False,levels = -1, get = "files", parts = "all", sort = True):
    """
    Get full path of files from all folders under the ``input_path`` (including itself).
    Can return specific files with optionnal conditions 
    Args:
        input_path (str): A valid path to a folder. 
            This folder is used as the root to return files found 
            (possible condition selection by giving to re_callback a function taking a regexp pattern and a string as argument, an returning a boolean).
    Returns:
        list: List of the file fullpaths found under ``input_path`` folder and subfolders.
    """
    #if levels = -1, we get  everything whatever the depth (at least up to 32767 subfolders, but this should be fine...)

    if levels == -1 :
        levels = 32767
    current_level = 0
    output_list = []
    
    def _recursive_search(_input_path):
        nonlocal current_level
        for subdir in os.listdir(_input_path):
            fullpath = os.path.join(_input_path,subdir)
            if os.path.isfile(fullpath): 
                if (get == "all" or get == "files") and (re_pattern is None or extract.qregexp(re_pattern,fullpath)):
                    output_list.append(os.path.normpath(fullpath))
                    
            else :
                if (get == "all" or get == "dirs" or get == "folders") and (re_pattern is None or extract.qregexp(re_pattern,fullpath)):
                    output_list.append(os.path.normpath(fullpath))
                if current_level < levels:
                    current_level += 1 
                    _recursive_search(fullpath)
        current_level -= 1
        
    if os.path.isfile(input_path):
        raise ValueError(f"Can only list files in a directory. A file was given : {input_path}")
 
    _recursive_search(input_path)
    
    if relative :
        output_list = [os.path.relpath(file,start = input_path) for file in output_list]
    if parts == "name" :
        output_list = [os.path.basename(file) for file in output_list]
    if sort :
        output_list = natsort.natsorted(output_list)
    return output_list
        

    
        