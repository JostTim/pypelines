from functools import wraps
from typing import TYPE_CHECKING
from contextlib import contextmanager
import builtins

if TYPE_CHECKING:
    from .pipelines import Pipeline
    from .steps import BaseStep


class BaseStepTaskManager:
    step: "BaseStep"
    backend: "BaseTaskBackend"

    def __init__(self, step, backend):
        """Initializes the class with the specified step and backend.

        Args:
            step: The step value to be assigned.
            backend: The backend value to be assigned.
        """
        self.step = step
        self.backend = backend

    def start(self, session, *args, **kwargs):
        """Start the session with the given arguments.

        Args:
            session: The session to start.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Raises:
            NotImplementedError: If the backend is not set.
        """
        if not self.backend:
            raise NotImplementedError


class BaseTaskBackend:

    task_manager_class = BaseStepTaskManager
    success: bool = False

    def __init__(self, parent: "Pipeline", **kwargs):
        """Initializes a PipelineNode object.

        Args:
            parent (Pipeline): The parent Pipeline object.
            **kwargs: Additional keyword arguments.
        """
        self.parent = parent

    def __bool__(self):
        """Return the boolean value of the object based on the success attribute."""
        return self.success

    def create_task_manager(self, step) -> "BaseStepTaskManager":
        """Create a task manager for the given step.

        Args:
            step: The step for which the task manager is created.

        Returns:
            BaseStepTaskManager: An instance of BaseStepTaskManager for the given step.
        """
        return self.task_manager_class(step, self)


class NoImport:
    def __getattr__(self, name):
        """Return the value of the specified attribute name."""
        return self

    def __getitem__(self, index):
        """Return the item at the specified index."""
        return self

    def __setattr__(self, name, value):
        """Set the attribute with the specified name to the given value.

        Args:
            name (str): The name of the attribute to be set.
            value (any): The value to be assigned to the attribute.
        """
        pass

    def __setitem__(self, index, value):
        """Set the value at the specified index in the object."""
        pass


@contextmanager
def mock_failed_imports():
    """Mocks failed imports by replacing the built-in __import__ function with a custom implementation that returns
    a NoImport object when an ImportError occurs.

    This function is intended to be used as a context manager with a 'try/finally' block to ensure
    that the original __import__ function is restored after the mocked imports are no longer needed.

    Example:
    with mock_failed_imports():
        # Code that may raise ImportError during import statements
    """
    original_import = builtins.__import__

    def custom_import(name, *args, **kwargs):
        """Custom import function that tries to import a module using the original import function.
        If the import fails, it returns a NoImport object.

        Args:
            name (str): The name of the module to import.
            *args: Additional positional arguments to pass to the original import function.
            **kwargs: Additional keyword arguments to pass to the original import function.

        Returns:
            The imported module if successful, otherwise a NoImport object.
        """
        try:
            return original_import(name, *args, **kwargs)
        except ImportError:
            return NoImport()

    builtins.__import__ = custom_import
    try:
        yield
    finally:
        builtins.__import__ = original_import
