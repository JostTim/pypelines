from functools import wraps
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .pipelines import Pipeline


class BaseTaskBackend:

    success: bool = False

    def __init__(self, parent: "Pipeline", **kwargs):
        self.parent = parent

    def __bool__(self):
        return self.success

    def register_step(self, step):
        wrapped_step = getattr(step, "queue", None)
        if wrapped_step is None:
            # do not register
            pass
        # registration code here

    def wrap_step(self, step):

        @wraps(step.generate)
        def wrapper(*args, **kwargs):
            return step.generate(*args, **kwargs)

        return wrapper
