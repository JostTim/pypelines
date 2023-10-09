

from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .pipe import BasePipe

class BasePipeline:

    def __init__(self,name):
        self.pipeline_name = name
        self.pipes = {}
        self.resolved = False
    
    def register_pipe(self, pipe_class : type) -> type:
        """Wrapper to instanciate and attache a a class inheriting from BasePipe it to the Pipeline instance.
        The Wraper returns the class without changing it."""
        instance = pipe_class(self)
        #print(f"Added instance of Pipe {instance.pipe_name} to instance of Pipeline {self.__class__.__name__} {self = }")
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

    def get_requirement_stack(self, instance, required_steps = None, parents = None, max_recursion = 100):

        if required_steps is None :
            required_steps = []
    
        if parents is None:
            parents = []

        self.resolve()

        if instance in parents :
            raise RecursionError(f"Circular import : {parents[-1]} requires {instance} wich exists in parents hierarchy : {parents}")
            
        parents.append(instance)
        if len(parents) > max_recursion :
            raise ValueError("Too much recursion, unrealistic number of pipes chaining. Investigate errors or increase max_recursion")
        
        for requirement in instance.requires:
            required_steps, requirement = self.get_requirement_stack(requirement, required_steps, parents, max_recursion)
            if not requirement in required_steps:
                required_steps.append(requirement)

        parents.pop(-1)
        
        return required_steps, instance
    
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
            