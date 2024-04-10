from dataclasses import dataclass
import hashlib, random, json, inspect, re
from abc import ABCMeta, abstractmethod

from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING


if TYPE_CHECKING:
    from .steps import BaseStep


@dataclass
class Version:
    pipe_name: str
    id: str
    detail: dict

    @property
    def deprecated(self):
        """Return the deprecated status of the object."""
        return self.detail["deprecated"]

    @property
    def function_hash(self):
        """Return the hash value of the function."""
        return self.detail["function_hash"]

    @property
    def step_name(self):
        """Return the name of the step."""
        return self.detail["step_name"]

    @property
    def creation_date(self):
        """Return the creation date of the object."""
        return self.detail["creation_date"]

    def update_function_hash(self, new_function_hash):
        """Update the function hash in the detail dictionary.

        Args:
            new_function_hash: The new function hash to be updated.

        Returns:
            None
        """
        self.detail["function_hash"] = new_function_hash

    def deprecate(self):
        """Mark the function as deprecated."""
        self.detail["deprecated"] = True

    def __str__(self):
        """Return a string representation of the object."""
        return self.id


class BaseVersionHandler(metaclass=ABCMeta):

    function_hash_remove = ["comments", " ", "\n"]

    def __init__(self, pipe, *args, **kwargs):
        """Initializes the class with the provided pipe.

        Args:
            pipe: The pipe object to be assigned.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        self.pipe = pipe

    def compare_function_hash(self, step):
        """Compares the function hash of the active version with the hash of the current function.

        Args:
            step: The step for which the function hash needs to be compared.

        Returns:
            bool: True if the function hashes match, False otherwise.
        """
        try:
            version = self.get_active_version(step)
        except KeyError:
            return False
        current_hash = self.get_function_hash(step.step)
        return version.function_hash == current_hash

    def get_function_hash(self, function) -> str:
        """Get the hash value of a function after removing specified elements.

        Args:
            function: The function for which the hash value needs to be calculated.

        Returns:
            str: The hash value of the function.
        """

        def remove_comments(source):
            """Remove all single-line and multi-line comments from the given source code.

            Args:
                source (str): The source code containing comments.

            Returns:
                str: The source code with all comments removed.
            """
            # remove all occurance of single-line comments (#comments) from the source
            source_no_comments = re.sub(r"#[^\n]*", "", source)
            # remove all occurance of multi-line comments ("""comment""") from the source
            source_no_comments = re.sub(r"\'\'\'.*?\'\'\'", "", source_no_comments, flags=re.DOTALL)
            source_no_comments = re.sub(r"\"\"\".*?\"\"\"", "", source_no_comments, flags=re.DOTALL)
            return source_no_comments

        remove = self.function_hash_remove
        source = inspect.getsource(function)

        if "comments" in remove:
            remove.pop(remove.index("comments"))
            source = remove_comments(source)

        for rem in remove:
            source = source.replace(rem, "")

        return hashlib.sha256(source.encode()).hexdigest()

    @abstractmethod
    def get_new_version_string(self) -> str:
        """Returns a new version string."""

    @abstractmethod
    def get_active_version(self, step: "BaseStep") -> Version:
        """Get the active version for a given step.

        Args:
            step (BaseStep): The step for which to retrieve the active version.

        Returns:
            Version: The active version for the given step.
        """

    @abstractmethod
    def apply_changes(self, versions) -> None:
        """Apply changes to the object based on the given versions.

        Args:
            versions (list): A list of versions containing the changes to be applied.

        Returns:
            None
        """


class HashVersionHandler(BaseVersionHandler):

    hash_collision_max_attempts = 3

    def __init__(self, pipe, file_path):
        """Initializes the class with the provided pipe and file path.

        Args:
            pipe: The pipe object to be used.
            file_path: The path to the file containing memory data.
        """
        super().__init__(pipe)
        self.path = file_path
        self.memory = json.load(open(file_path, "r"))
        self.verify_structure(pipe.pipeline)

    def get_new_version_string(self) -> str:
        """Generate a new unique version string by creating a hash and checking for collisions.

        Returns:
            str: A new unique version string.

        Raises:
            ValueError: If a unique hash cannot be determined after the maximum attempts.
        """
        max_attempts = self.hash_collision_max_attempts
        for i in range(max_attempts):  # max no-collision attempts, then raises error

            m = hashlib.sha256()
            r = str(random.random()).encode()
            m.update(r)
            new_hash = m.hexdigest()[0:7]

            if new_hash not in self.memory["versions"].keys():
                return new_hash

        raise ValueError(
            "Could not determine a unique hash not colliding with existing values. "
            "Please investigate code / step_architecture.json file ?"
        )

    def apply_changes(self, versions):
        """Apply changes to the memory based on the provided versions.

        Args:
            versions (list or object): A list of versions or a single version object.

        Returns:
            None
        """
        if not isinstance(versions, list):
            versions = [versions]

        for version in versions:
            try:
                edited_object = self.memory["versions"][version.id]
            except KeyError:
                self.steps_dict[version.pipe_name] = self.steps_dict.get(
                    version.pipe_name, {"versions": {}, "step_renamings": {}}
                )
                edited_object = self.steps_dict[version.pipe_name]["versions"][version.id] = self.steps_dict[
                    version.pipe_name
                ]["versions"].get(version.id, {})
            edited_object.update(version.detail)

    def verify_structure(self, pipeline):
        """Verify the structure of the pipeline by iterating through each pipe and step."""
        for pipe_name, pipe in pipeline.pipes.items():
            for step_name, step in pipe.steps.items():
                pass
                # in here, check function hash of the current implementation matches the one in the version,
                # or send a warning to user that he may update the version or ignor by updating the function
                # hash and keeping the same version
