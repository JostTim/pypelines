import numpy as np
import matplotlib.pyplot as plt

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from networkx import DiGraph


class PipelineGraph:
    callable_graph: "DiGraph"
    name_graph: "DiGraph"

    def __init__(self, pipeline):
        from networkx import DiGraph, draw, spring_layout, draw_networkx_labels

        self.pipeline = pipeline
        self.pipeline.resolve()

        self.DiGraph = DiGraph
        self.nxdraw = draw
        self.get_spring_layout = spring_layout
        self.draw_networkx_labels = draw_networkx_labels

        self.make_graphs()

    def make_graphs(self):

        callable_graph = self.DiGraph()
        display_graph = self.DiGraph()
        for pipe in self.pipeline.pipes.values():
            for step in pipe.steps.values():
                callable_graph.add_node(step)
                display_graph.add_node(step.relative_name)
                for req in step.requires:
                    callable_graph.add_edge(req, step)
                    display_graph.add_edge(req.relative_name, step.relative_name)

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
        if ax is None:
            _, ax = plt.subplots(figsize=figsize)
        if layout == "aligned":
            pos = self.get_aligned_layout()
        elif layout == "spring":
            pos = self.get_spring_layout(self.name_graph)
        else:
            raise ValueError("layout must be : aligned or tree")

        labels = self.get_labels(line_return, remove_pipe)
        if remove_pipe:
            self.draw_columns_labels(pos, ax, font_size=font_size, rotation=rotation)
        pos = self.separate_crowded_levels(pos, max_spacing=max_spacing)
        self.nxdraw(self.name_graph, pos, ax=ax, with_labels=False, node_color=node_color, **kwargs)
        texts = self.draw_networkx_labels(self.name_graph, pos, labels, font_size=font_size)
        for _, t in texts.items():
            t.set_rotation(rotation)
        ax.margins(0.20)
        ax.set_title(f"Pipeline {self.pipeline.pipeline_name} requirement graph", y=0.05)
        return ax

    def draw_columns_labels(self, pos, ax, font_size=7, rotation=30):
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
            pos[node.relative_name] = (x, -y)
        return pos

    def separate_crowded_levels(self, pos, max_spacing=0.35):
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
