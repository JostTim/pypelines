from .pipes import BasePipe
from .steps import BaseStep
from .disk import BaseDiskObject

import pickle, natsort, os, re, logging
import pandas as pd


IGNORE_VERSIONS = False


class PickleDiskObject(BaseDiskObject):
    collection = ["preprocessing_saves"]  # collection a.k.a subfolders in the session.path
    extension = "pickle"
    current_suffixes = ""
    remove = True
    current_disk_file = None
    update_file_format = True
    is_legacy_format = False

    def __init__(self, session, step, extra=""):
        """Initialize the StepTask object.

        Args:
            session: The session object for the task.
            step: The step object for the task.
            extra: Additional information for the task (default is an empty string).
        """
        self.file_prefix = step.pipeline.pipeline_name
        super().__init__(session, step, extra)

    def version_deprecated(self) -> bool:
        """Check if the current version is deprecated.

        This method compares the current version with the disk version and returns True if they are different,
        indicating that the current version is deprecated. Otherwise, it returns False.

        Returns:
            bool: True if the current version is deprecated, False otherwise.
        """
        logger = logging.getLogger("pickle.version_deprecated")

        if IGNORE_VERSIONS:
            return False

        # if we didn't found the disk version, we return False.
        # it's not labeled as "deprecated" for retro-compatibility
        if self.disk_version is None or self.disk_version == "":
            return False

        if self.version != self.disk_version:
            logger.debug(
                f"Disk version {self.disk_version} was different than current version {self.version}. Returning True"
            )
            return True

        logger.debug(
            f"Disk version {self.disk_version} was identicall to current version {self.version}. Returning False"
        )
        return False

    def step_level_too_low(self) -> bool:
        """Check if the level of the disk step is lower than the current step level.

        Returns:
            bool: True if the disk step level is lower than the current step level, False otherwise.
        """
        logger = logging.getLogger("pickle.step_level_too_low")

        # we get the step instance that corresponds to the one on the disk
        disk_step = self.disk_step_instance()

        # if we didn't found the disk step, we return False.
        # it's not labeled as "too low" for retro-compatibility
        if disk_step is None:
            return False

        # we compare levels with the currently called step
        # if disk step level < current called step level, we return True, else we return False.
        if disk_step.get_level(selfish=True) < self.step.get_level(selfish=True):
            logger.debug(
                f"Disk step {disk_step.relative_name} was lower than {self.step.relative_name}. Returning True"
            )
            return True

        logger.debug(
            f"Disk step {disk_step.relative_name} was higher or equal than {self.step.relative_name}. Returning False"
        )
        return False

    @property
    def version(self):
        """Return the version of the pipeline."""
        return self.step.pipe.version

    def parse_extra(self, extra, regexp=False):
        """Parses the extra string by optionally applying a regular expression pattern.

        Args:
            extra (str): The extra string to be parsed.
            regexp (bool): A flag indicating whether to apply regular expression pattern (default is False).

        Returns:
            str: The parsed extra string.
        """
        extra = extra.strip(".")
        if regexp:
            extra = extra.replace(".", r"\.")
            extra = r"\." + extra if extra else ""
        else:
            extra = r"." + extra if extra else ""
        return extra

    def make_file_name_pattern(self):
        """Generate a file name pattern based on the steps in the pipeline.

        Returns:
            str: A regular expression pattern for creating file names based on the steps in the pipeline.
        """
        steps_patterns = []

        for key in sorted(self.step.pipe.steps.keys()):
            step = self.step.pipe.steps[key]
            steps_patterns.append(rf"(?:{step.step_name})")

        steps_patterns = "|".join(steps_patterns)

        version_pattern = r"(?:\.(?P<version>[^\.]*))?"
        step_pattern = rf"(?:\.(?P<step_name>{steps_patterns}){version_pattern})?"

        extra = self.parse_extra(self.extra, regexp=True)

        pattern = self.file_prefix + r"\." + self.step.pipe_name + step_pattern + extra + r"\." + self.extension
        return pattern

    def get_file_name(self):
        """Return the file name based on the object attributes.

        Returns:
            str: The generated file name.
        """
        extra = self.parse_extra(self.extra, regexp=False)
        version_string = "." + self.version if self.version else ""
        filename = (
            self.file_prefix
            + "."
            + self.step.pipe_name
            + "."
            + self.step.step_name
            + version_string
            + extra
            + "."
            + self.extension
        )
        return filename

    def check_disk(self):
        """Check disk for matching files based on specified pattern and expected values.

        Returns:
            bool: True if a matching file is found, False otherwise.
        """
        logger = logging.getLogger("pickle.check_disk")

        search_path = os.path.join(self.session.path, os.path.sep.join(self.collection))
        pattern = self.make_file_name_pattern()

        os.makedirs(search_path, exist_ok=True)

        logger.debug(f"Searching at folder : {search_path} with {pattern=}")
        matching_files = files(search_path, re_pattern=pattern, relative=True, levels=0)
        logger.debug(f"Found files : {matching_files}")

        if not len(matching_files):
            return False

        keys = ["step_name", "version"]
        expected_values = {
            "step_name": self.step.step_name,
            "version": self.version if self.version else None,
        }
        cpattern = re.compile(pattern)
        match_datas = []
        for index, file in enumerate(matching_files):
            match = cpattern.search(file)
            match_data = {}
            for key in keys:
                match_data[key] = match.group(key)
                # TODO DEBUG: catch here with KeyError and return an error that is more explicit, if key is
                # not present in the automatically generated re pattern (would be a BUG)

            if expected_values == match_data:
                self.current_disk_file = os.path.join(search_path, matching_files[index])
                self.disk_version = match_data["version"]
                self.disk_step = match_data["step_name"]
                logger.debug(
                    f"Matched a single file : {self.current_disk_file} with {self.disk_step=} {self.disk_version=}"
                )
                return True
            match_datas.append(match_data)

        if len(match_datas) == 1:
            logger.load(
                f"A single partial match was found for {self.object_name}. Please make sure it is consistant with"
                f" expected behaviour. Expected : {expected_values}, Found : {match_datas[0]}"
            )
            self.current_disk_file = os.path.join(search_path, matching_files[0])
            self.disk_version = match_datas[0]["version"]
            self.disk_step = match_datas[0]["step_name"]
            if self.disk_version is None and self.disk_step is None:
                self.is_legacy_format = True
            return True
        else:
            logger.load(
                f"More than one partial match was found for {self.step.relative_name}. Cannot auto select. Expected :"
                f" {expected_values}, Found : {match_datas}"
            )
            return False

    def get_found_disk_object_description(self):
        """Return the description of the found disk object."""
        return str(self.current_disk_file)

    def get_full_path(self):
        """Return the full path of the file by joining the session path, collection, and file name.

        Returns:
            str: The full path of the file.
        """
        full_path = os.path.join(self.session.path, os.path.sep.join(self.collection), self.get_file_name())
        return full_path

    def save(self, data):
        """Save data to disk.

        Args:
            data: Data to be saved to disk. If data is a pandas DataFrame, it will be saved as a pickle file.
                Otherwise, it will be pickled and saved.

        Returns:
            None
        """
        logger = logging.getLogger("PickleDiskObject.save")
        new_full_path = self.get_full_path()
        logger.debug(f"Saving to path : {new_full_path}")

        if isinstance(data, pd.DataFrame):
            data.to_pickle(new_full_path)
        else:
            with open(new_full_path, "wb") as f:
                pickle.dump(data, f)
        if self.current_disk_file is not None and self.current_disk_file != new_full_path and self.remove:
            logger.debug(f"Removing old file from path : {self.current_disk_file}")
            try:
                os.remove(self.current_disk_file)
            except FileNotFoundError:
                logger.error(f"The file {self.current_disk_file} that should have been removed don't exist anymore")
        self.current_disk_file = new_full_path

    def load(self):
        """Load data from the current disk file.

        Raises:
            IOError: If no file was found on disk or 'check_disk()' was not run.

        Returns:
            The loaded data from the disk file.
        """
        logger = logging.getLogger("PickleDiskObject.load")
        logger.debug(f"Current disk file status : {self.current_disk_file=}")
        if self.current_disk_file is None:
            raise IOError(
                "Could not find a file to load. Either no file was found on disk, or you forgot to run 'check_disk()'"
            )

        try:
            with open(self.current_disk_file, "rb") as f:
                data = pickle.load(f)
        except ModuleNotFoundError as e:
            logger.debug("Unable to load using generick pickling")
            if "pandas" in e.__str__():
                logger.debug("Trying out pandas read_pickle")
                data = pd.read_pickle(self.current_disk_file)
            else:
                logger.debug(f"Pandas not found in {e.__str__()}. Raising error")
                raise e

        if self.update_file_format and self.is_legacy_format:
            self.save(data)
            self.is_legacy_format = False

        return data

    @staticmethod
    def multisession_packer(sessions, session_result_dict: dict) -> pd.DataFrame | dict:
        """Packs the results of multiple sessions into a DataFrame
            if all values in the session_result_dict are DataFrames.

        Args:
            sessions: List of sessions.
            session_result_dict (dict): Dictionary containing the results of each session.

        Returns:
            pd.DataFrame or dict: Returns a DataFrame if all values in session_result_dict are DataFrames,
                otherwise returns the original session_result_dict.
        """
        session_result_dict = BaseDiskObject.multisession_packer(sessions, session_result_dict)

        are_dataframe = [isinstance(item, pd.core.frame.DataFrame) for item in session_result_dict.values()]

        if not all(are_dataframe):
            return session_result_dict

        return PickleDiskObject.get_multi_session_df(session_result_dict, add_session_level=False)

    @staticmethod
    def get_multi_session_df(multisession_data_dict: dict, add_session_level: bool = False) -> pd.DataFrame:
        """Return a pandas DataFrame by combining multiple session dataframes.

        Args:
            multisession_data_dict (dict): A dictionary containing session names as keys and dataframes as values.
            add_session_level (bool, optional): Whether to add session level to the index. Defaults to False.

        Returns:
            pd.DataFrame: A combined dataframe containing data from all sessions.
        """
        dataframes = []
        for session_name, dataframe in multisession_data_dict.items():
            level_names = list(dataframe.index.names)

            dataframe = dataframe.reset_index()

            if add_session_level:
                dataframe["session#"] = [session_name] * len(dataframe)
                dataframe = dataframe.set_index(["session#"] + level_names, inplace=False)

            else:
                level_0_copy = dataframe[level_names[0]].copy()
                dataframe[level_names[0].replace("#", "")] = level_0_copy
                dataframe["session"] = [session_name] * len(dataframe)

                dataframe[level_names[0]] = dataframe[level_names[0]].apply(
                    PickleDiskObject.merge_index_element, session_name=session_name
                )
                dataframe = dataframe.set_index(level_names)

            dataframes.append(dataframe)

        multisession_dataframe = pd.concat(dataframes)
        return multisession_dataframe

    @staticmethod
    def merge_index_element(values: tuple | str | float | int, session_name: str) -> tuple:
        """Merge the elements of the input values with the session name.

        Args:
            values (tuple | str | float | int): The values to be merged with the session name.
            session_name (str): The name of the session to be merged with the values.

        Returns:
            tuple: A tuple containing the merged values with the session name.
        """
        if not isinstance(values, tuple):
            values = (values,)

        new_values = []
        for value in values:
            value = str(value) + "_" + session_name
            new_values.append(value)

        if len(new_values) == 1:
            return new_values[0]
        return tuple(new_values)


class PicklePipe(BasePipe):
    # single_step = False
    step_class = BaseStep
    disk_class = PickleDiskObject


def files(
    input_path,
    re_pattern=None,
    relative=False,
    levels=-1,
    get="files",
    parts="all",
    sort=True,
):
    """
    Get full path of files from all folders under the ``input_path`` (including itself).
    Can return specific files with optionnal conditions
    Args:
        input_path (str): A valid path to a folder.
            This folder is used as the root to return files found
            (possible condition selection by giving to re_callback a function taking a regexp pattern and a string as
            argument, an returning a boolean).
    Returns:
        list: List of the file fullpaths found under ``input_path`` folder and subfolders.
    """
    # if levels = -1, we get  everything whatever the depth
    # (up to 32767 subfolders, so this should be fine...)

    if levels == -1:
        levels = 32767
    current_level = 0
    output_list = []

    def _recursive_search(_input_path):
        """Recursively search for files and directories in the given input path.

        Args:
            _input_path (str): The input path to start the recursive search from.

        Returns:
            None
        """
        nonlocal current_level
        for subdir in os.listdir(_input_path):
            fullpath = os.path.join(_input_path, subdir)
            if os.path.isfile(fullpath):
                if (get == "all" or get == "files") and (re_pattern is None or qregexp(re_pattern, fullpath)):
                    output_list.append(os.path.normpath(fullpath))

            else:
                if (get == "all" or get == "dirs" or get == "folders") and (
                    re_pattern is None or qregexp(re_pattern, fullpath)
                ):
                    output_list.append(os.path.normpath(fullpath))
                if current_level < levels:
                    current_level += 1
                    _recursive_search(fullpath)
        current_level -= 1

    if os.path.isfile(input_path):
        raise ValueError(f"Can only list files in a directory. A file was given : {input_path}")

    if not os.path.isdir(input_path):
        # the given directory does not exist, we return an empty list to notify no file was found
        return []

    _recursive_search(input_path)

    if relative:
        output_list = [os.path.relpath(file, start=input_path) for file in output_list]
    if parts == "name":
        output_list = [os.path.basename(file) for file in output_list]
    if sort:
        output_list = natsort.natsorted(output_list)
    return output_list


def qregexp(regex, input_line, groupidx=None, matchid=None, case=False):
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

    if case:
        matches = re.finditer(regex, input_line, re.MULTILINE | re.IGNORECASE)
    else:
        matches = re.finditer(regex, input_line, re.MULTILINE)

    if matchid is not None:
        matchid = matchid + 1

    for matchnum, match in enumerate(matches, start=1):
        if matchid is not None:
            if matchnum == matchid:
                if groupidx is not None:
                    for groupx, groupcontent in enumerate(match.groups()):
                        if groupx == groupidx:
                            return groupcontent
                    return False

                else:
                    MATCH = match.group()
                    return MATCH

        else:
            if groupidx is not None:
                for groupx, groupcontent in enumerate(match.groups()):
                    if groupx == groupidx:
                        return groupcontent
                return False
            else:
                MATCH = match.group()
                return MATCH
    return False
