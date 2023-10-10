from dataclasses import dataclass
import hashlib, random, json, inspect, re

from typing import Callable, Type, Iterable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .steps import BaseStep

@dataclass
class Version:
    pipe_name : str
    id : str
    detail : dict

    @property
    def deprecated(self) :
        return self.detail["deprecated"]

    @property
    def function_hash(self) :
        return self.detail["function_hash"]

    @property
    def step_name(self) :
        return self.detail["step_name"]

    @property
    def creation_date(self) :
        return self.detail["creation_date"]

    def update_function_hash(self, new_function_hash):
        self.detail["function_hash"] = new_function_hash

    def deprecate(self):
        self.detail["deprecated"] = True

    def __str__(self):
        return self.id
    
class BaseVersionHandler:

    function_hash_remove = ["comments", " ", "\n"]

    def __init__(self, pipe, *args, **kwargs):
        self.pipe = pipe

    def compare_function_hash(self, step):
        try :
            version = self.get_active_version(step)
        except KeyError :
            return False
        current_hash = self.get_function_hash(step.step)
        return version.function_hash == current_hash
    
    def get_function_hash(self, function) -> str :

        def remove_comments(self, source):
            # remove all occurance of single-line comments (#comments) from the source
            source_no_comments = re.sub(r'#[^\n]*', '', source)
            # remove all occurance of multi-line comments ("""comment""") from the source
            source_no_comments = re.sub(r'\'\'\'.*?\'\'\'', '', source_no_comments, flags=re.DOTALL)
            source_no_comments = re.sub(r'\"\"\".*?\"\"\"', '', source_no_comments, flags=re.DOTALL)
            return source_no_comments

        remove = self.function_hash_remove
        source = inspect.getsource(function)
    
        if "comments" in remove :
            remove.pop(remove.index("comments"))
            source = remove_comments(source)
            
        for rem in remove :
            source = source.replace(rem, "")
    
        return hashlib.sha256(source.encode()).hexdigest()

    def get_new_version_string(self) -> str :
        ...

    def get_active_version(self, step : "BaseStep") -> Version :
        ...

    def apply_changes(self, versions) -> None :
        ...

class HashVersionHandler(BaseVersionHandler):

    hash_collision_max_attempts = 3

    def __init__(self, pipe, file_path) :
        super().__init__(pipe)
        self.path = file_path
        self.memory = json.load(open(file_path,"r"))
        self.verify_structure()

    def get_new_version_string(self) -> str :
        max_attempts = self.hash_collision_max_attempts
        for i in range(max_attempts):# max no-collision attempts, then raises error
        
            m = hashlib.sha256()
            r = str(random.random()).encode()
            m.update(r)
            new_hash = m.hexdigest()[0:7]
            
            if new_hash not in self.memory["versions"].keys():
                return new_hash
            
        raise ValueError("Could not determine a unique hash not colliding with existing values. Please investigate code / step_architecture.json file ?") 

    def apply_changes(self, versions ):
        if not isinstance(versions, list) :
            versions = [versions]

        for version in versions :
            try : 
                edited_object = self.memory["versions"][version.id]
            except KeyError:
                self.steps_dict[version.pipe_name] =  self.steps_dict.get(version.pipe_name,{"versions":{},"step_renamings":{}})
                edited_object = self.steps_dict[version.pipe_name]["versions"][version.id] = self.steps_dict[version.pipe_name]["versions"].get(version.id,{})
            edited_object.update(version.detail) 

    def verify_structure(self, pipeline):
        for pipe_name, pipe in pipeline.pipes.items():
            for step_name, step in pipe.steps.items():
                pass
                #in here, check function hash of the current implementation matches the one in the version, or send a warning to user that he may update the version or ignor by updating the function hash and keeping the same version