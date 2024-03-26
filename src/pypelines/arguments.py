from functools import wraps
from logging import getLogger
import json, re, os


def read_json_file(json_file: str):
    """Loads a Json file that can have some comments indicated with // after a line.

    Args:
        json_file (str): file_path

    Returns:
        dict: The python dictionnary corresponding to the json file's data. Comments are not included.
    """
    # loading the file to a string
    with open(json_file, "r") as file:
        json_string = file.read()

    # removing the comments
    pattern = r"//[^\n{}]+"
    json_no_comments = re.sub(pattern, "", json_string, flags=re.DOTALL)

    # loading the json format string without comments as a python dict
    return json.loads(json_no_comments)


def read_session_arguments_file(session, step, file_suffix="_arguments.json"):
    file_name = step.pipeline.pipeline_name + file_suffix
    try:
        path = os.path.join(session.path, file_name)
        return read_json_file(path)
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not open the config file {file_name} for the session {session.alias}")


def autoload_arguments(wrapped_function, step):
    """Loads arguments for a pipeline step, using a config file unique to a given session.
    Arguments supplied to the function by the user will override the arguments that have been set by the autoloader.

    Args:
        wrapped_function (_type_): step_worker_function
        step (_type_): the step instance from wich to get name information.

    Returns:
        function: the wrapped function
    """

    @wraps(wrapped_function)
    def wraper(session, *args, **kwargs):
        local_log = getLogger("autoload_arguments")

        config_kwargs = get_step_arguments(session, step)
        if config_kwargs:  # new_kwargs is not empty
            local_log.note(
                f"Using the arguments for the function {step.relative_name} found in pipelines_arguments.json."
            )
        # this loop is just to show to log wich arguments have been overriden
        # from the json config by some arguments in the code
        overrides_names = []
        for key in config_kwargs.keys():
            if key in kwargs.keys():
                overrides_names.append(key)

        if overrides_names:
            local_log.note(
                f"Values of pipelines_arguments.json arguments : {', '.join(overrides_names)}, are overrided by the"
                f" current call arguments to {step.relative_name}"
            )

        config_kwargs.update(kwargs)
        return wrapped_function(session, *args, **config_kwargs)

    return wraper


def get_step_arguments(session, step):
    local_log = getLogger("autoload_arguments")

    try:
        config_args = read_session_arguments_file(session, step)["functions"][step.relative_name]
    except FileNotFoundError as e:
        local_log.debug(f"{type(e).__name__} : {e}. Skipping")
        return {}
    except KeyError:
        local_log.debug(
            f"Could not find the `functions` key or the key `{step.relative_name}` in pipelines_arguments.json file at"
            f" {session.path}. Skipping"
        )
        return {}

    return config_args
