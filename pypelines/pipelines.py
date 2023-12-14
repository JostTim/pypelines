from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .pipes import BasePipe


class Pipeline:
    def __init__(self, name):
        self.pipeline_name = name
        self.pipes = {}
        self.resolved = False

    def register_pipe(self, pipe_class: type) -> type:
        """Wrapper to instanciate and attache a a class inheriting from BasePipe it to the Pipeline instance.
        The Wraper returns the class without changing it."""
        instance = pipe_class(self)

        # attaches the instance itself to the pipeline, and to the dictionnary 'pipes' of the current pipeline
        if instance.single_step:
            # in case it's a single_step instance (speficied by the user, not auto detected)
            # then we attach the step to the pipeline directly as a pipe, for ease of use.
            step = list(instance.steps.values())[0]
            self.pipes[instance.pipe_name] = step
            # just add steps to the step instance serving as a pipe, so that it behaves
            # similarly to a pipe for some pipelines function requiring this attribute to exist.
            step.steps = instance.steps
            setattr(self, instance.pipe_name, step)
        else:
            # in case it's a pipe, we attach it in a simple manner.
            self.pipes[instance.pipe_name] = instance
            setattr(self, instance.pipe_name, instance)

        self.resolved = False
        return pipe_class

    def resolve_instance(self, instance_name: str):
        pipe_name, step_name = instance_name.split(".")
        try:
            pipe = self.pipes[pipe_name]
            if pipe.single_step:
                return pipe
            return pipe.steps[step_name]
        except KeyError:
            raise KeyError(f"No instance {instance_name} has been registered to the pipeline")

    def resolve(self):
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

    def get_requirement_stack(self, instance, names=False, max_recursion=100):
        self.resolve()
        parents = []
        required_steps = []

        def recurse_requirement_stack(
            instance,
        ):  # , required_steps = None, parents = None):
            # if required_steps is None :
            #     required_steps = []

            # if parents is None:
            #     parents = []

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
                # required_steps =
                recurse_requirement_stack(requirement)  # , required_steps, parents, max_recursion)
                if requirement not in required_steps:
                    required_steps.append(requirement)

            parents.pop(-1)

            # return required_steps

        recurse_requirement_stack(instance)
        if names:
            required_steps = [req.full_name for req in required_steps]
        return required_steps

    @property
    def graph(self):
        return PipelineGraph(self)


class PipelineGraph:
    callable_graph = None
    name_graph = None

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.pipeline.resolve()

        self.make_graphs()

    def make_graphs(self):
        from networkx import DiGraph

        callable_graph = DiGraph()
        display_graph = DiGraph()
        for pipe in self.pipeline.pipes.values():
            for step in pipe.steps.values():
                callable_graph.add_node(step)
                display_graph.add_node(step.full_name)
                for req in step.requires:
                    callable_graph.add_edge(req, step)
                    display_graph.add_edge(req.full_name, step.full_name)

        self.callable_graph = callable_graph
        self.name_graph = display_graph

    def draw(
        self,
        font_size=7,
        layout="aligned",
        ax=None,
        figsize=(12, 7),
        line_return=True,
        remove_pipe=True,
        rotation=18,
        max_spacing=0.28,
        node_color="orange",
        **kwargs,
    ):
        from networkx import draw, spring_layout, draw_networkx_labels
        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots(figsize=figsize)
        if layout == "aligned":
            pos = self.get_aligned_layout()
        elif layout == "spring":
            pos = spring_layout(self.name_graph)
        else:
            raise ValueError("layout must be : aligned or tree")

        labels = self.get_labels(line_return, remove_pipe)
        if remove_pipe:
            self.draw_columns_labels(pos, ax, font_size=font_size, rotation=rotation)
        pos = self.separate_crowded_levels(pos, max_spacing=max_spacing)
        draw(self.name_graph, pos, ax=ax, with_labels=False, node_color=node_color, **kwargs)
        texts = draw_networkx_labels(self.name_graph, pos, labels, font_size=font_size)
        for _, t in texts.items():
            t.set_rotation(rotation)
        ax.margins(0.20)
        ax.set_title(f"Pipeline {self.pipeline.pipeline_name} requirement graph", y=0.05)
        return ax

    def draw_columns_labels(self, pos, ax, font_size=7, rotation=30):
        import numpy as np

        unique_pos = {}
        for key, value in pos.items():
            column = key.split(".")[0]
            if column in unique_pos.keys():
                continue
            unique_pos[column] = (value[0], 1)

        for column_name, (x, y) in unique_pos.items():
            ax.text(
                x, y, column_name, ha="center", va="center", fontsize=font_size, rotation=rotation, fontweight="bold"
            )
            ax.axvline(x, ymin=0.1, ymax=0.85, zorder=-1, lw=0.5, color="gray")

    def get_labels(self, line_return=True, remove_pipe=True):
        labels = {}
        for node_name in self.name_graph.nodes:
            formated_name = node_name
            if remove_pipe:
                formated_name = formated_name.split(".")[1]
            if line_return:
                formated_name = formated_name.replace(".", "\n")
            labels[node_name] = formated_name
        return labels

    def get_aligned_layout(self):
        pipe_x_indices = {pipe.pipe: index for index, pipe in enumerate(self.pipeline.pipes.values())}
        pos = {}
        for node in self.callable_graph.nodes:
            # if len([]) # TODO : add distinctions of fractions of y if multiple nodes of the same pipe have same level
            x = pipe_x_indices[node.pipe]
            y = node.get_level()
            pos[node.full_name] = (x, -y)
        return pos

    def separate_crowded_levels(self, pos, max_spacing=0.35):
        import numpy as np

        treated_pipes = []
        for key, value in pos.items():
            pipe_name = key.split(".")[0]
            x_pos = value[0]
            y_pos = value[1]
            if f"{pipe_name}_{y_pos}" in treated_pipes:
                continue
            multi_steps = {k: v for k, v in pos.items() if pipe_name == k.split(".")[0] and v[1] == y_pos}
            if len(multi_steps) == 1:
                continue
            x_min, x_max = x_pos - max_spacing, x_pos + max_spacing
            new_xs = np.linspace(x_min, x_max, len(multi_steps))
            for new_x, (k, (x, y)) in zip(new_xs, multi_steps.items()):
                pos[k] = (new_x, y)

            treated_pipes.append(f"{pipe_name}_{y_pos}")

        return pos
