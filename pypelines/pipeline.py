




class BasePipeline:

    pipes = {}
    
    def register_pipe(self,pipe_class):
        pipe_class(self)

        return pipe_class
    
    def __init__(self, versions = None):
        self.versions = versions
        
    def resolve(self, instance_name : str) :
        pipe_name , step_name = instance_name.split(".")
        try :
            pipe = self.pipes[pipe_name]
            if pipe.single_step :
                return pipe
            return pipe.steps[step_name]
        except KeyError :
            raise KeyError(f"No instance {instance_name} has been registered to the pipeline")

    def get_requirement_stack(self, instance, required_steps = None, parents = None, max_recursion = 100):
        if required_steps is None :
            required_steps = []
    
        if parents is None:
            parents = []
            
        if isinstance(instance,str):
            instance = self.resolve(instance)
    
        if instance in parents :
            raise RecursionError(f"Circular import : {parents[-1]} requires {instance} wich exists in parents hierarchy : {parents}")
            
        parents.append(instance)
        if len(parents) > max_recursion :
            raise ValueError("Too much recursion, unrealistic number of pipes chaining. Investigate errors or increase max_recursion")
        instanciated_requires = []
        
        for requirement in instance.requires:
            required_steps, requirement = self.get_requirement_stack(requirement, required_steps, parents, max_recursion)
            if not requirement in required_steps:
                required_steps.append(requirement)
            instanciated_requires.append(requirement)
            
        instance.requires = instanciated_requires
        parents.pop(-1)
        
        return required_steps, instance