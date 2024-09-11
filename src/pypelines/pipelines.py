from logging import getLogger
import os
from .tasks import BaseTaskBackend

from typing import Callable, Type, Dict, List, Iterable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .pipes import BasePipe
    from .steps import BaseStep
    from .graphs import PipelineGraph


class Pipeline:
    pipes: Dict[str, "BasePipe"]
    runner_backend_class = BaseTaskBackend

    def __init__(self, name: str, **runner_args):
        """Initialize the pipeline with the given name and runner arguments.

        Args:
            name (str): The name of the pipeline.
            **runner_args: Additional keyword arguments for the runner backend.

        Attributes:
            pipeline_name (str): The name of the pipeline.
            pipes (dict): Dictionary to store pipeline components.
            resolved (bool): Flag to indicate if the pipeline is resolved.
            runner_backend: The runner backend object created with the provided arguments.
                If creation fails, it evaluates to False as a boolean.
        """
        self.pipeline_name = name
        self.pipes = {}
        self.resolved = False

        # create a runner backend, if fails, the runner_backend object evaluates to False as a boolean
        # (to be checked and used througout the pipeline wrappers creation)
        self.runner_backend = self.runner_backend_class(self, **runner_args)

    def register_pipe(self, pipe_class: Type["BasePipe"]) -> Type["BasePipe"]:
        """Wrapper to instanciate and attache a a class inheriting from BasePipe it to the Pipeline instance.
        The Wraper returns the class without changing it.
        """
        instance = pipe_class(self)

        # attaches the instance itself to the pipeline, and to the dictionnary 'pipes' of the current pipeline
        # if instance.single_step:
        #     # in case it's a single_step instance (speficied by the user, not auto detected)
        #     # then we attach the step to the pipeline directly as a pipe, for ease of use.
        #     step = list(instance.steps.values())[0]
        #     self.pipes[instance.pipe_name] = step
        #     # just add steps to the step instance serving as a pipe, so that it behaves
        #     # similarly to a pipe for some pipelines function requiring this attribute to exist.
        #     step.steps = instance.steps
        #     setattr(self, instance.pipe_name, step)
        # else:
        # in case it's a pipe, we attach it in a simple manner.
        self.pipes[instance.pipe_name] = instance
        setattr(self, instance.pipe_name, instance)

        self.resolved = False
        return pipe_class

    def resolve_instance(self, instance_name: str) -> "BaseStep":
        """Resolve the specified step instance name to a BaseStep object,
        looking at the pipe and step names separated by a comma.

        Args:
            instance_name (str): The name of the instance in the format 'pipe_name.step_name'.

        Returns:
            BaseStep: The BaseStep object corresponding to the instance name.

        Raises:
            KeyError: If the specified instance name is not found in the pipeline.
        """
        pipe_name, step_name = instance_name.split(".")
        try:
            pipe = self.pipes[pipe_name]
            # if pipe.single_step:
            #    return pipe
            return pipe.steps[step_name]
        except KeyError as exc:
            raise KeyError(f"No instance {instance_name} has been registered to the pipeline") from exc

    def resolve(self) -> None:
        """Scans currentely registered Pipes.
        Ensures that for each Pipe's Step, the items in requires list are Step instances, and not strings.
        If they aren't instanciate them.
        Once ran, sets a flag resolved to True, to avoid needing to reprocess the class's Pipes.
        This flag is set to False inside register_pipe function, if a new class gets registered.
        """
        if self.resolved:
            return

        for pipe in self.pipes.values():
            for step in pipe.steps.values():
                instanciated_requires = []
                for req in step.requires:
                    if isinstance(req, str):
                        req = self.resolve_instance(req)
                    instanciated_requires.append(req)

                step.requires = instanciated_requires

        self.resolved = True

    def __getattr__(self, name: str) -> "BasePipe":
        if name in self.pipes:
            return self.pipes[name]
        raise AttributeError(f"'Pipeline' object has no attribute '{name}'")

    def get_requirement_stack(
        self, instance: "BaseStep", names: bool = False, max_recursion: int = 100
    ) -> List["BaseStep"]:
        """Returns a list containing the ordered Steps that the "instance" Step object requires for being ran.

        Args:
            instance (BaseStep): _description_
            names (bool, optional): _description_. Defaults to False.
            max_recursion (int, optional): _description_. Defaults to 100.

        Raises:
            RecursionError: _description_
            ValueError: _description_

        Returns:
            list: _description_
        """

        self.resolve()  # ensure requires lists are containing instances and not strings
        parents: List["BaseStep"] = []
        required_steps = []

        def recurse_requirement_stack(
            instance: "BaseStep",
        ):
            """
            _summary_

            Args:
                instance (BaseStep): _description_

            Raises:
                RecursionError: _description_
                ValueError: _description_
            """
            if instance in parents:
                raise RecursionError(
                    f"Circular import : {parents[-1]} requires {instance} wich exists in parents hierarchy : {parents}"
                )

            parents.append(instance)
            if len(parents) > max_recursion:
                raise ValueError(
                    "Too much recursion, unrealistic number of pipes chaining. Investigate errors or increase"
                    " max_recursion"
                )

            for requirement in instance.requires:
                recurse_requirement_stack(requirement)
                if requirement not in required_steps:
                    required_steps.append(requirement)

            parents.pop(-1)

        recurse_requirement_stack(instance)
        if names:
            required_steps = [req.relative_name for req in required_steps]
        return required_steps

    @property
    def graph(self) -> "PipelineGraph":
        """Return a PipelineGraph object representing the graph of the pipeline."""
        from .graphs import PipelineGraph

        return PipelineGraph(self)
