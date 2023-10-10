from .pipes import BasePipe
from .steps import BaseStep
from .disk import BaseDiskObject

import pickle, natsort, os, re, logging

class PickleDiskObject(BaseDiskObject) :

    collection = ["preprocessing_saves"] #collection a.k.a subfolders in the session.path
    extension = "pickle"
    current_suffixes = ""
    remove = True
    current_disk_file = None

    def __init__(self, session, step, extra = "") :
        self.file_prefix = step.pipeline.pipeline_name
        super().__init__(session, step, extra)

    #TODO : IMPLEMENT BOTH OF THEESE FOR REAL
    def version_deprecated(self) -> bool :
        return False

    def step_missing_requirements(self) -> bool :
        return False

    def parse_extra(self,extra, regexp = False):
        extra = extra.strip(".")
        if regexp :
            extra = extra.replace(".",r"\.")
            extra = r"\." + extra if extra else ""
        else :
            extra = r"." + extra if extra else ""
        return extra

    def make_file_name_pattern(self):

        steps_patterns = []

        for key in sorted(self.step.pipe.steps.keys()):

            step = self.step.pipe.steps[key]
            steps_patterns.append( fr"(?:{step.step_name})" )

        steps_patterns = "|".join(steps_patterns)

        version_pattern = fr"(?:\.(?P<version>[^\.]*))?"
        step_pattern = fr"(?:\.(?P<step_name>{steps_patterns}){version_pattern})?"
        
        extra = self.parse_extra(self.extra, regexp = True)
                
        pattern = self.file_prefix + r"\." + self.step.pipe_name + step_pattern + extra + r"\." + self.extension
        return pattern
    
    def get_file_name(self):

        extra = self.parse_extra(self.extra, regexp = False)
        version_string = "." + self.step.version if self.step.version else ""
        filename = self.file_prefix + "." + self.step.pipe_name + "." + self.step.step_name + version_string + extra + "." + self.extension
        return filename

    def check_disk(self):
        logger = logging.getLogger("pickle.check_disk")

        search_path = os.path.join(self.session.path, os.path.sep.join(self.collection))
        pattern = self.make_file_name_pattern()
        
        logger.debug(f"Searching at folder : {search_path} with {pattern=}")
        matching_files = files(search_path, re_pattern = pattern, relative = True, levels = 0)
        logger.debug(f"Found files : {matching_files}")

        if not len(matching_files):
            return False
 
        keys = ["step_name","version"]
        expected_values = {"step_name" : self.step.step_name, "version" : self.step.version if self.step.version else None}
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
                self.disk_version = match_data["version"]
                self.disk_step = match_data["step_name"]
                logger.debug(f"Matched a single file : {self.current_disk_file} with {self.disk_step=} {self.disk_version=}")
                return True
            match_datas.append(match_data)
        
        if len(match_datas) == 1:
            logger.warning(f"A single partial match was found. Please make sure it is consistant with expected behaviour. Expected : {expected_values} , Found : {match_datas[0]}")
            self.current_disk_file = os.path.join(search_path, matching_files[0])
            self.disk_version = match_datas[0]["version"]
            self.disk_step = match_datas[0]["step_name"]
            return True
        else :
            logger.warning(f"More than one partial match were found. Cannot auto select. Expected : {expected_values} , Found : {match_datas}")
            return False

    
    def get_full_path(self):
        full_path = os.path.join(self.session.path, os.path.sep.join(self.collection), self.get_file_name() )
        return full_path

    def save(self, data):
        logger = logging.getLogger("save")
        new_full_path = self.get_full_path()
        logger.debug(f"Saving to path : {new_full_path}")
        with open(new_full_path, "wb") as f :
            pickle.dump(data, f)
        if self.current_disk_file is not None and self.current_disk_file != new_full_path and self.remove :
            logger.debug(f"Removing old file from path : {self.current_disk_file}")
            os.remove(self.current_disk_file)
        self.current_disk_file = new_full_path

    def load(self) :
        logger = logging.getLogger("save")
        logger.debug(f"Current disk file status : {self.current_disk_file = }")
        if self.current_disk_file is None :
            raise IOError("Could not find a file to load. Either no file was found on disk, or you forgot to run 'check_disk()'")
        with open(self.current_disk_file, "rb") as f :
            return pickle.load(f)
class PicklePipe(BasePipe):
    
    single_step = False
    step_class = BaseStep
    disk_class = PickleDiskObject

    def disk_step(self):
        return None

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
        