import os, re
from . sessions import Session
import pickle

from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .step import BaseStep

class OutputData(Protocol):
    """Can be a mapping, iterable, single element, or None.

    This class is defined for typehints, and is not a real class useable at runtime"""

class BaseDiskObject :

    disk_version = None
    disk_step = None

    def __init__(self, session : Session, step : "BaseStep", extra = "") -> None :

        self.session = session
        self.step = step
        self.extra = extra

        self.check_disk()

    def check_disk(self):
        """sets self.disk_version and self.disk_step"""
        ...

    def save(self, data : OutputData) -> None:
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
    remove = True
    current_disk_file = None

    def parse_extra(self,extra):
        extra = extra.strip(".").replace(".",r"\.")
        return r"\." + extra if extra else ""

    def make_file_name_pattern(self):

        steps_patterns = []

        for key in sorted(self.step.pipe.steps.keys()):

            step = self.step.pipe.steps[key]
            steps_patterns.append( fr"(?:{step.step_name})" )

        steps_patterns = "|".join(steps_patterns)

        version_pattern = fr"(?:\.(?P<version>[^\.]*))?"
        step_pattern = fr"(?:\.(?P<step_name>{steps_patterns}){version_pattern})?"
        
        extra = self.parse_extra(self.extra)
                
        pattern = self.file_prefix + r"\." + self.step.pipe_name + step_pattern + extra + r"\." + self.extension
        print(pattern)
        return pattern
    
    def get_file_name(self):

        extra = self.parse_extra(self.extra)
        version_string = "." + self.step.version if self.step.use_version else ""
        filename = self.file_prefix + "." + self.step.pipe_name + "." + self.step.step_name + version_string + extra + "." + self.extension
        return filename

    def check_disk(self):
        search_path = os.path.join(self.session.path, os.path.sep.join(self.collection))
        print(search_path)
        matching_files = files(search_path, re_pattern = self.make_file_name_pattern(), relative = True, levels = 0)
        print(matching_files)
        if len(matching_files):
            keys = ["step_name","version"]
            expected_values = {"step_name" : self.step.step_name, "version" : self.step.version if self.step.use_version else None}
            pattern = re.compile(self.make_file_name_pattern())
            match_datas = []
            for index, file in enumerate(matching_files) :
                match = pattern.search(file)
                match_data = {}
                for key in keys :
                    match_data[key] = match.group(key)
                    #TODO : catch here with KeyError and return an error that is more explicit, saying key is not present in the pattern
                if expected_values == match_data :
                    self.current_disk_file = os.path.join(search_path, matching_files[index])
                    return True
                match_datas.append(match_data)
            else :            
                if len(match_datas) == 1:
                    print(f"A single partial match was found. Please make sure it is consistant with expected behaviour. Expected : {expected_values} , Found : {match_datas[0]}") 
                    self.current_disk_file = os.path.join(search_path, matching_files[0])
                    return True
                print(f"More than one partial match were found. Cannot auto select. Expected : {expected_values} , Found : {match_datas}")   
                return False
        return False
    
    def get_full_path(self):
        full_path = os.path.join(self.session.path, os.path.sep.join(self.collection), self.get_file_name() )
        return full_path

    def save(self, data : OutputData):
        new_full_path = self.get_full_path()
        with open(new_full_path, "wb") as f :
            pickle.dump(data, f)
        if self.current_disk_file is not None and self.current_disk_file != new_full_path and self.remove :
            os.remove(self.current_disk_file)
        self.current_disk_file = new_full_path

    def load(self) -> OutputData:
        if self.current_disk_file is None :
            raise IOError("Could not find a file to load. Either no file was found on disk, or you forgot to run 'check_disk()'")
        with open(self.current_disk_file, "rb") as f :
            return pickle.load(f)

import natsort

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
                if (get == "all" or get == "files") and (re_pattern is None or qregexp(re_pattern,fullpath)):
                    output_list.append(os.path.normpath(fullpath))
                    
            else :
                if (get == "all" or get == "dirs" or get == "folders") and (re_pattern is None or qregexp(re_pattern,fullpath)):
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
        

    
def qregexp(regex, input_line, groupidx=None, matchid=None , case=False):
    """
    Simplified implementation for matching regular expressions. Utility for python's built_in module re .

    Tip:
        Design your patterns easily at [Regex101](https://regex101.com/)

    Args:
        input_line (str): Source on wich the pattern will be searched.
        regex (str): Regex pattern to match on the source.
        **kwargs (optional):
            - groupidx : (``int``)
                group index in case there is groups. Defaults to None (first group returned)
            - matchid : (``int``)
                match index in case there is multiple matchs. Defaults to None (first match returned)
            - case : (``bool``)
                `False` / `True` : case sensitive regexp matching (default ``False``)

    Returns:
        Bool , str: False or string containing matched content.

    Warning:
        This function returns only one group/match.

    """

    if case :
        matches = re.finditer(regex, input_line, re.MULTILINE|re.IGNORECASE)
    else :
        matches = re.finditer(regex, input_line, re.MULTILINE)

    if matchid is not None :
        matchid = matchid +1

    for matchnum, match in enumerate(matches,  start = 1):

        if matchid is not None :
            if matchnum == matchid :
                if groupidx is not None :
                    for groupx, groupcontent in enumerate(match.groups()):
                        if groupx == groupidx :
                            return groupcontent
                    return False

                else :
                    MATCH = match.group()
                    return MATCH

        else :
            if groupidx is not None :
                for groupx, groupcontent in enumerate(match.groups()):
                    if groupx == groupidx :
                        return groupcontent
                return False
            else :
                MATCH = match.group()
                return MATCH
    return False
        