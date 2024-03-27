from functools import wraps
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .pipelines import Pipeline
    from .steps import BaseStep


class BaseStepTaskManager:
    step: "BaseStep"
    backend: "BaseTaskBackend"

    def __init__(self, step, backend):
        self.step = step
        self.backend = backend

    def start(self, session, *args, **kwargs):
        if not self.backend:
            raise NotImplementedError


class BaseTaskBackend:

    task_manager_class = BaseStepTaskManager
    success: bool = False

    def __init__(self, parent: "Pipeline", **kwargs):
        self.parent = parent

    def __bool__(self):
        return self.success

    def create_task_manager(self, step) -> "BaseStepTaskManager":
        return self.task_manager_class(step, self)
