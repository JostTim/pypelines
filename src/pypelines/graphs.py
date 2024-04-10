from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from networkx import DiGraph


class PipelineGraph:
    callable_graph: "DiGraph"
    name_graph: "DiGraph"

    def __init__(self, pipeline):
        """Initialize the PipelineVisualizer object.

        Args:
            pipeline: The pipeline object to visualize.

        Returns:
            None
        """
        from networkx import DiGraph, draw, spring_layout, draw_networkx_labels

        self.pipeline = pipeline
        self.pipeline.resolve()

        self.DiGraph = DiGraph
        self.nxdraw = draw
        self.get_spring_layout = spring_layout
        self.draw_networkx_labels = draw_networkx_labels

        self.make_graphs()

    def make_graphs(self):
        """Generates two directed graphs based on the pipeline steps.

        This method creates two directed graphs: callable_graph and display_graph.
        The callable_graph represents the pipeline steps and their dependencies.
        The display_graph represents the pipeline steps with their relative names.

        Returns:
            None
        """

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
        """Draws a requirement graph using NetworkX and Matplotlib.

        Args:
            font_size (int): Font size for node labels (default is 7).
            layout (str): Layout type for the graph, either "aligned" or "spring" (default is "aligned").
            ax (matplotlib.axes.Axes): Matplotlib axes to draw the graph on (default is None).
            figsize (tuple): Figure size for the plot (default is (12, 7)).
            line_return (bool): Whether to include line return in node labels (default is True).
            remove_pipe (bool): Whether to remove pipe characters from node labels (default is True).
            rotation (int): Rotation angle for node labels (default is 18).
            max_spacing (float): Maximum spacing between nodes (default is 0.28).
            node_color (str): Color for the nodes (default is "orange").
            **kwargs: Additional keyword arguments to be passed to NetworkX drawing functions.

        Returns:
            matplotlib.axes.Axes: The matplotlib axes containing the drawn graph.
        """
        if ax is None:
            import matplotlib.pyplot as plt

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
        """Draw column labels on the plot.

        Args:
            pos (dict): A dictionary containing the positions of the columns.
            ax (matplotlib.axes.Axes): The axes object on which to draw the labels.
            font_size (int, optional): The font size of the labels. Defaults to 7.
            rotation (int, optional): The rotation angle of the labels in degrees. Defaults to 30.

        Returns:
            None
        """
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
        """Return formatted labels for nodes in the graph.

        Args:
            line_return (bool): Whether to replace '.' with '\n' in the formatted name. Default is True.
            remove_pipe (bool): Whether to remove everything before the first '.' in the formatted name.
                Default is True.

        Returns:
            dict: A dictionary containing node names as keys and their formatted names as values.
        """
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
        """Return the layout of nodes in a graph with aligned x-coordinates and negative y-coordinates.

        Returns:
            dict: A dictionary mapping node names to their (x, y) coordinates in the layout.
        """
        pipe_x_indices = {pipe.pipe: index for index, pipe in enumerate(self.pipeline.pipes.values())}
        pos = {}
        for node in self.callable_graph.nodes:
            # if len([]) # TODO : add distinctions of fractions of y if multiple nodes of the same pipe have same level
            x = pipe_x_indices[node.pipe]
            y = node.get_level()
            pos[node.relative_name] = (x, -y)
        return pos

    def separate_crowded_levels(self, pos, max_spacing=0.35):
        """Separate crowded levels by adjusting the x positions of pipes with the same y position.

        Args:
            pos (dict): A dictionary containing the positions of pipes in the format {pipe_name: (x_pos, y_pos)}.
            max_spacing (float, optional): The maximum spacing allowed between pipes on the same level.
                Defaults to 0.35.

        Returns:
            dict: A dictionary with adjusted positions to separate crowded levels.
        """
        from numpy import linspace

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
            new_xs = linspace(x_min, x_max, len(multi_steps))
            for new_x, (k, (x, y)) in zip(new_xs, multi_steps.items()):
                pos[k] = (new_x, y)

            treated_pipes.append(f"{pipe_name}_{y_pos}")

        return pos
