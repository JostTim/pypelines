

from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .pipes import BasePipe

class Pipeline:

    def __init__(self,name):
        self.pipeline_name = name
        self.pipes = {}
        self.resolved = False
    
    def register_pipe(self, pipe_class : type) -> type:
        """Wrapper to instanciate and attache a a class inheriting from BasePipe it to the Pipeline instance.
        The Wraper returns the class without changing it."""
        instance = pipe_class(self)
        
        # attaches the instance itself to the pipeline, and to the dictionnary 'pipes' of the current pipeline
        if instance.single_step :
            # in case it's a single_step instance (speficied by the user, not auto detected) 
            # then we attach the step to the pipeline directly as a pipe, for ease of use.
            step = list(instance.steps.values())[0]
            self.pipes[instance.pipe_name] = step
            # just add steps to the step instance serving as a pipe, so that it behaves 
            # similarly to a pipe for some pipelines function requiring this attribute to exist.
            step.steps = instance.steps 
            setattr(self, instance.pipe_name, step)
        else :
            # in case it's a pipe, we attach it in a simple manner.
            self.pipes[instance.pipe_name] = instance
            setattr(self, instance.pipe_name, instance)
        
        self.resolved = False
        return pipe_class
        
    def resolve_instance(self, instance_name : str) :
        pipe_name , step_name = instance_name.split(".")
        try :
            pipe = self.pipes[pipe_name]
            if pipe.single_step :
                return pipe
            return pipe.steps[step_name]
        except KeyError :
            raise KeyError(f"No instance {instance_name} has been registered to the pipeline")
        
    def resolve(self):
        if self.resolved:
            return
        
        for pipe in self.pipes.values() :

            for step in pipe.steps.values() :
                instanciated_requires = []
                for req in step.requires :
                    if isinstance(req,str):
                        req = self.resolve_instance(req)
                    instanciated_requires.append(req)

                
                step.requires = instanciated_requires 
            
        self.resolved = True


    def get_requirement_stack(self, instance, names = False, max_recursion = 100):

        self.resolve()
        parents = []
        required_steps = []

        def recurse_requirement_stack(instance):#, required_steps = None, parents = None):

            # if required_steps is None :
            #     required_steps = []
        
            # if parents is None:
            #     parents = []

            if instance in parents :
                raise RecursionError(f"Circular import : {parents[-1]} requires {instance} wich exists in parents hierarchy : {parents}")
                
            parents.append(instance)
            if len(parents) > max_recursion :
                raise ValueError("Too much recursion, unrealistic number of pipes chaining. Investigate errors or increase max_recursion")
            
            for requirement in instance.requires:
                #required_steps = 
                recurse_requirement_stack(requirement)#, required_steps, parents, max_recursion)
                if not requirement in required_steps:
                    required_steps.append(requirement)

            parents.pop(-1)
            
            #return required_steps
    
        recurse_requirement_stack(instance)
        if names : 
            required_steps = [ req.full_name for req in required_steps ]
        return required_steps
    
    def get_graph(self):
        from networkx import DiGraph

        self.resolve()

        callable_graph = DiGraph()
        display_graph = DiGraph()
        for pipe in self.pipes.values() :

            for step in pipe.steps.values() :
                callable_graph.add_node(step)
                display_graph.add_node(step.full_name)
                for req in step.requires :
                    callable_graph.add_edge(req, step)
                    display_graph.add_edge(req.full_name, step.full_name)

        return callable_graph, display_graph
            
    def draw_graph(self, font_size = 7, x_spacing = 1, layout = "aligned"):

        import networkx as nx
        import matplotlib.pyplot as plt

        Gfunc, Gname = self.get_graph()

        if layout == "aligned" :
            pos = self.get_aligned_layout(Gfunc, x_spacing = x_spacing)
        elif layout == "tree" :
            pos = self.get_tree_layout(Gname, x_spacing = x_spacing)
        else :
            raise ValueError("layout must be : aligned or tree")
        
        nx.draw(Gname, pos, with_labels=True, font_size = font_size)
        ax = plt.gca()
        ax.margins(0.20)
        return ax

    def get_aligned_layout(self, Gfunc, x_spacing = 1):
        pipe_x_indices = { pipe.pipe : index for index, pipe in enumerate(self.pipes.values())}
        pos = {}
        for node in Gfunc.nodes :
            x = pipe_x_indices[node.pipe]
            y = node.get_level()
            pos[node.full_name] = (x*x_spacing,-y)
        return pos

    def get_tree_layout(self, G, x_spacing = 1):
        ### Doesn't work so great

        from collections import deque

        roots = [node for node in G.nodes if G.in_degree(node) == 0]
        if not roots:
            raise ValueError("Error: graph has no roots!")

        pos = {}
        base_x = 0
        next_x = 0
        for root in roots:
            qx = deque([next_x + xt for xt in range(0, G.out_degree(root))])
            qy = deque([0 - 1]*G.out_degree(root))
            next_x = base_x

            visited = {root}
            deque_content = [(root, iter(G[root]), qx, qy)]
            #print(deque_content)
            #print(list(iter(G[root])))
            queue = deque(deque_content)

            while queue:
                parent, children, qx, qy = queue.popleft()
                #print(children)
                for child in children:
                    #print(child)

                    if child not in visited:
                        x = qx.popleft()
                        y = qy.popleft()
                        pos[child] = (x*x_spacing, y)
                        visited.add(child)
                        qx_child = deque([x+ xt for xt in range(0, G.out_degree(child))])
                        qy_child = deque([y - 1]*G.out_degree(child))
                        queue.append((child, iter(G[child]), qx_child, qy_child))
                
            pos[root] = (base_x, max([val[1] for val in pos.values()])+1) # place the root
            base_x = base_x + max([val[0] for val in pos.values()]) + 1

        return pos




    