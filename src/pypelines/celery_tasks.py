from .tasks import BaseTaskBackend, BaseStepTaskManager
from .pipelines import Pipeline
from .loggs import FileFormatter
from pathlib import Path
from traceback import format_exc as format_traceback_exc
import logging
import coloredlogs
from logging import getLogger
from platform import node
from pandas import Series
import platform
from threading import Thread

from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from celery import Celery
    from .steps import BaseStep


APPLICATIONS_STORE = {}


class CeleryAlyxTaskManager(BaseStepTaskManager):

    backend: "CeleryTaskBackend"
    step: "BaseStep"

    def register_step(self):
        """Register a step in the backend.

        This method registers a task in the backend using the runner obtained from get_runner() method.

        Returns:
            None
        """
        if self.backend:
            # self.backend.app.task(CeleryRunner, name=self.step.complete_name)
            self.backend.app.register_task(self.get_runner())

    def start(self, session, extra=None, **kwargs):
        """Starts a task on a celery cluster.

        Args:
            session: The session to use for the task.
            extra: Extra information to pass to the task (default is None).
            **kwargs: Additional keyword arguments to pass to the task.

        Raises:
            NotImplementedError: If the pipeline does not have a working celery backend.

        Returns:
            The created CeleryTaskRecord.
        """

        if not self.backend:
            raise NotImplementedError(
                "Cannot start a task on a celery cluster as this pipeline " "doesn't have a working celery backend"
            )

        return CeleryTaskRecord.create(self, session, extra, **kwargs)

    def get_runner(superself):  # type: ignore
        """Return a CeleryRunner task for executing a step in a pipeline.

        Args:
            superself: The parent object that contains the step information.

        Returns:
            CeleryRunner: A Celery Task object that runs the specified step.

        Raises:
            Any exceptions that occur during the execution of the task.
        """
        from celery import Task

        class CeleryRunner(Task):
            name = superself.step.complete_name

            def run(self, task_id, extra=None):

                task = CeleryTaskRecord(task_id)

                try:
                    session = task.get_session()
                    application = task.get_application()

                    with LogTask(task) as log_object:
                        logger = log_object.logger
                        task["log"] = log_object.filename
                        task["status"] = "Started"
                        task.partial_update()

                        try:
                            step: "BaseStep" = (
                                application.pipelines[task.pipeline_name].pipes[task.pipe_name].steps[task.step_name]
                            )

                            step.generate(session, extra=extra, **task.arguments, **task.management_arguments)
                            task.status_from_logs(log_object)
                        except Exception as e:
                            traceback_msg = format_traceback_exc()
                            logger.critical(f"Fatal Error : {e}")
                            logger.critical("Traceback :\n" + traceback_msg)
                            task["status"] = "Failed"

                except Exception as e:
                    # if it fails outside of the nested try statement, we can't store logs files,
                    # and we mention the failure through alyx directly.
                    task["status"] = "Uncatched_Fail"
                    task["log"] = str(e)

                task.partial_update()

        return CeleryRunner


class CeleryTaskRecord(dict):
    session: Series

    # a class to make dictionnary keys accessible with attribute syntax
    def __init__(self, task_id, task_infos_dict={}, response_handle=None, session=None):
        """Initialize the Task object.

        Args:
            task_id (str): The unique identifier for the task.
            task_infos_dict (dict, optional): A dictionary containing information about the task.
                If not provided, it will be fetched using the task_id. Defaults to {}.
            response_handle (Any, optional): A handle for the response. Defaults to None.
            session (Any, optional): A session object. Defaults to None.
        """

        if not task_infos_dict:
            from one import ONE

            connector = ONE(mode="remote", data_access_mode="remote")
            task_infos_dict = connector.alyx.rest("tasks", "read", id=task_id)

        super().__init__(task_infos_dict)
        self.session = session  # type: ignore
        self.response = response_handle

    def status_from_logs(self, log_object):
        """Update the status based on the content of the log file.

        Args:
            log_object: Log object containing the full path to the log file.

        Returns:
            None
        """
        with open(log_object.fullpath, "r") as f:
            content = f.read()

        if len(content) == 0:
            status = "No_Info"
        elif "CRITICAL" in content:
            status = "Failed"
        elif "ERROR" in content:
            status = "Errors"
        elif "WARNING" in content:
            status = "Warnings"
        else:
            status = "Complete"

        self["status"] = status

    def partial_update(self):
        """Partially updates a task using the ONE API.

        This function connects to the ONE database in remote mode and performs a partial update on a task
        using the export data from the current instance.

        Returns:
            None
        """
        from one import ONE

        connector = ONE(mode="remote", data_access_mode="remote")
        connector.alyx.rest("tasks", "partial_update", **self.export())

    def get_session(self):
        """Retrieve the session object associated with the current instance.

        Returns:
            The session object.
        """
        if self.session is None:
            from one import ONE

            connector = ONE(mode="remote", data_access_mode="remote")
            session = connector.search(id=self["session"], no_cache=True, details=True)
            self.session = session  # type: ignore

        return self.session

    def get_application(self):
        """Return the application associated with the executable stored in the instance.

        Returns:
            str: The application associated with the executable.

        Raises:
            KeyError: If the application associated with the executable is not found in the APPLICATIONS_STORE.
        """
        try:
            return APPLICATIONS_STORE[self["executable"]]
        except KeyError:
            raise KeyError(f"Unable to retrieve the application {self['executable']}")

    @property
    def pipeline_name(self):
        """Return the name of the pipeline by splitting the name attribute at '.' and returning the first part."""
        return self["name"].split(".")[0]

    @property
    def pipe_name(self):
        """Return the name of the pipe by splitting the name attribute using '.' and returning the second element.

        Returns:
            str: The name of the pipe.
        """
        return self["name"].split(".")[1]

    @property
    def step_name(self):
        """Return the third element after splitting the 'name' attribute of the object with '.'."""
        return self["name"].split(".")[2]

    @property
    def arguments(self):
        """Retrieve and filter arguments for the current step.

        Returns:
            dict: Filtered arguments for the current step.
        """
        # once step arguments control will be done via file, these should take prio over the main step ran's file args
        args = self.get("arguments", {})
        args = args if args else {}
        management_args = self.management_arguments
        filtered_args = {}
        for key, value in args.items():
            if key not in management_args.keys():
                filtered_args[key] = value
        return filtered_args

    @property
    def management_arguments(self):
        """Returns a dictionary of management arguments based on the default values and any provided arguments.

        Returns:
            dict: A dictionary containing the management arguments with keys:
                - "skip": A boolean indicating whether to skip management.
                - "refresh": A boolean indicating whether to refresh.
                - "refresh_requirements": A boolean indicating whether to refresh requirements.
                - "check_requirements": A boolean indicating whether to check requirements.
                - "save_output": A boolean indicating whether to save output.
        """
        default_management_args = {
            "skip": True,
            "refresh": False,
            "refresh_requirements": False,
            "check_requirements": True,
            "save_output": True,
        }
        args = self.get("arguments", {})
        management_args = {}
        for key, default_value in default_management_args.items():
            management_args[key] = args.get(key, default_value)

        if management_args["refresh"] == True:
            management_args["skip"] = False

        return management_args

    @property
    def session_path(self) -> str:
        """Returns the path of the session."""
        return self.session["path"]

    @property
    def task_id(self):
        """Return the task ID."""
        return self["id"]

    def export(self):
        """Export the object as a dictionary with specific keys removed.

        Returns:
            dict: A dictionary containing the object's id and data with certain keys removed.
        """
        return {"id": self["id"], "data": {k: v for k, v in self.items() if k not in ["id", "session_path"]}}

    @staticmethod
    def create(task_manager: CeleryAlyxTaskManager, session, extra=None, **kwargs):
        """Creates a new task using the given CeleryAlyxTaskManager and session.

        Args:
            task_manager (CeleryAlyxTaskManager): The CeleryAlyxTaskManager instance to use.
            session: The session to associate with the task.
            extra (optional): Any extra information to include in the task.
            **kwargs: Additional keyword arguments to pass to the task.

        Returns:
            CeleryTaskRecord: A CeleryTaskRecord object representing the created task.
        """
        from one import ONE

        connector = ONE(mode="remote", data_access_mode="remote")

        data = {
            "session": session.name,
            "name": task_manager.step.complete_name,
            "arguments": kwargs,
            "status": "Waiting",
            "executable": str(task_manager.backend.app.main),
        }

        task_dict = connector.alyx.rest("tasks", "create", data=data)

        worker = task_manager.backend.app.tasks[task_manager.step.complete_name]
        response_handle = worker.delay(task_dict["id"], extra=extra)

        return CeleryTaskRecord(
            task_dict["id"], task_infos_dict=task_dict, response_handle=response_handle, session=session
        )

    @staticmethod
    def create_from_task_name(app: "Celery", task_name: str, pipeline_name: str, session, extra=None, **kwargs):
        """Create a new task from the given task name and pipeline name.

        Args:
            app (Celery): The Celery application.
            task_name (str): The name of the task to be created.
            pipeline_name (str): The name of the pipeline.
            session: The session object.
            extra (optional): Extra information for the task.
            **kwargs: Additional keyword arguments.

        Returns:
            CeleryTaskRecord: A record of the created Celery task.
        """
        from one import ONE

        connector = ONE(mode="remote", data_access_mode="remote")

        data = {
            "session": session.name if isinstance(session, Series) else session,
            "name": task_name,
            "arguments": kwargs,
            "status": "Waiting",
            "executable": pipeline_name,
        }

        task_dict = connector.alyx.rest("tasks", "create", data=data)

        response_handle = app.send_task(name=task_name, kwargs={"task_id": task_dict["id"], "extra": extra})

        return CeleryTaskRecord(
            task_dict["id"], task_infos_dict=task_dict, response_handle=response_handle, session=session
        )

    @staticmethod
    def create_from_model(
        app: "Celery", task_model: type, task_name: str, pipeline_name: str, session: object, extra=None, **kwargs
    ):
        """Create a new task from a given task model and send it to a Celery app.

        Args:
            app (Celery): The Celery app instance.
            task_model (type): The task model class to create a new task instance.
            task_name (str): The name of the task.
            pipeline_name (str): The name of the pipeline.
            session (object): The session object.
            extra (optional): Extra information to pass to the task.
            **kwargs: Additional keyword arguments to pass to the task.

        Returns:
            CeleryTaskRecord: A record of the created task with task ID, task information dictionary,
                response handle, and session.
        """

        new_task = task_model(name=task_name, session=session, arguments=kwargs, status=25, executable=pipeline_name)
        new_task.save()

        task_dict = new_task.__dict__.copy()
        task_dict.pop("_state", None)

        response_handle = app.send_task(name=task_name, kwargs={"task_id": task_dict["id"], "extra": extra})

        return CeleryTaskRecord(
            task_dict["id"], task_infos_dict=task_dict, response_handle=response_handle, session=session
        )


class CeleryTaskBackend(BaseTaskBackend):
    app: "Celery"
    task_manager_class = CeleryAlyxTaskManager

    def __init__(self, parent: Pipeline, app: "Celery | None" = None):
        """Initialize the PipelineApp object.

        Args:
            parent (Pipeline): The parent Pipeline object.
            app (str): The Celery app associated with the Pipeline, or None if not provided.

        Attributes:
            parent (Pipeline): The parent Pipeline object.
            success (bool): Flag indicating if the initialization was successful.
            app (str): The Celery app associated with the Pipeline.
        """
        super().__init__(parent)
        self.parent = parent

        if app is not None:
            self.success = True
            self.app = app

            pipelines = getattr(self.app, "pipelines", {})
            pipelines[parent.pipeline_name] = parent
            self.app.pipelines = pipelines

    def start(self):
        """Starts the application."""
        self.app.start()

    def create_task_manager(self, step):
        """Create a task manager for the given step.

        Args:
            step: The step to be associated with the task manager.

        Returns:
            Task manager object initialized with the given step.
        """
        task_manager = self.task_manager_class(step, self)
        task_manager.register_step()
        return task_manager


class CeleryPipeline(Pipeline):
    runner_backend_class = CeleryTaskBackend


class LogTask:
    def __init__(self, task_record: CeleryTaskRecord, username=None, level="LOAD"):
        """Initialize the TaskLogger object.

        Args:
            task_record (CeleryTaskRecord): The Celery task record.
            username (str, optional): The username associated with the task. Defaults to None.
            level (str, optional): The logging level for the task. Defaults to "LOAD".
        """
        self.path = Path(task_record.session_path) / "logs"
        self.username = username if username is not None else (node() if node() else "unknown")
        self.worker_pk = task_record.task_id
        self.task_name = task_record["name"]
        self.level = getattr(logging, level.upper())

    def __enter__(self):
        """Perform necessary setup tasks when entering a context manager.

        Returns:
            self: The current instance of the context manager.
        """
        self.path.mkdir(exist_ok=True)
        self.logger = getLogger()
        self.set_handler()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up resources when exiting a context manager.

        Args:
            exc_type: The type of the exception that caused the exit, or None if no exception occurred.
            exc_val: The exception instance that caused the exit, or None if no exception occurred.
            exc_tb: The traceback of the exception that caused the exit, or None if no exception occurred.
        """
        self.remove_handler()

    def set_handler(self):
        """Set up logging handler for the current task.

        This method sets up a logging handler for the current task by creating a log file
        with a specific filename based on task details.
        It then configures the file handler with appropriate formatters and filters for colored logging.

        Returns:
            None
        """
        self.filename = f"task_log.{self.task_name}.{self.worker_pk}.log"
        self.fullpath = self.path / self.filename
        fh = logging.FileHandler(self.fullpath)
        f_formater = FileFormatter()
        coloredlogs.HostNameFilter.install(
            fmt=f_formater.FORMAT,
            handler=fh,
            style=f_formater.STYLE,
            use_chroot=True,
        )
        coloredlogs.ProgramNameFilter.install(
            fmt=f_formater.FORMAT,
            handler=fh,
            programname=self.task_name,
            style=f_formater.STYLE,
        )
        coloredlogs.UserNameFilter.install(
            fmt=f_formater.FORMAT,
            handler=fh,
            username=self.username,
            style=f_formater.STYLE,
        )

        fh.setLevel(self.level)
        fh.setFormatter(f_formater)
        self.logger.addHandler(fh)

    def remove_handler(self):
        """Removes the last handler from the logger."""
        self.logger.removeHandler(self.logger.handlers[-1])


def create_celery_app(conf_path, app_name="pypelines", v_host=None) -> "Celery | None":
    """Create a Celery app with the given configuration.

    Args:
        conf_path (str): The path to the configuration file.
        app_name (str): The name of the Celery app. Default is "pypelines".
        v_host (str): The virtual host for the Celery app.

    Returns:
        Celery | None: The created Celery app instance or None if creation failed.

    """

    failure_message = (
        f"Celery app : {app_name} failed to be created."
        "Don't worry, about this alert, "
        "this is not be an issue if you didn't explicitely planned on using celery. Issue was : "
    )

    logger = getLogger("pypelines.create_celery_app")

    if app_name in APPLICATIONS_STORE.keys():
        logger.warning(f"Tried to create a celery app named {app_name}, but it already exists. Returning it instead.")
        return APPLICATIONS_STORE[app_name]

    try:
        from celery import Task
    except ImportError as e:
        logger.warning(f"{failure_message} Could not import celery app. {e}")
        return None

    from types import MethodType

    def get_setting_files_path(conf_path) -> List[Path]:
        """Get the paths of setting files for the given configuration path.

        Args:
            conf_path (str): The path to the configuration file.

        Returns:
            List[Path]: A list of Path objects representing the setting files found.
        """
        conf_path = Path(conf_path)
        if conf_path.is_file():
            conf_path = conf_path.parent
        files = []
        for prefix, suffix in zip(["", "."], ["", "_secrets"]):
            file_loc = conf_path / f"{prefix}celery_{app_name}{suffix}.toml"
            if file_loc.is_file():
                files.append(file_loc)
        return files

    def get_signature_as_string(signature):
        """Return the function signature as a string without the 'session' parameter.

        Args:
            signature: The signature of the function.

        Returns:
            str: The function signature as a string without the 'session' parameter.
        """
        params = [
            param_value for param_name, param_value in signature.parameters.items() if param_name not in ["session"]
        ]
        return str(signature.replace(parameters=params))[1:-1].replace(" *,", "")

    def get_type_name(annotation):
        """Returns the name of the type hint for a given annotation.

        Args:
            annotation: The annotation for which to determine the type name.

        Returns:
            str: The name of the type hint.
        """
        from inspect import Parameter
        from typing import get_args, get_origin
        from types import UnionType

        if isinstance(annotation, str):
            annotation = string_to_typehint(annotation, globals(), locals())

        if isinstance(annotation, UnionType):
            typ = get_args(annotation)[0]
        elif hasattr(annotation, "__origin__"):  # For types from 'typing' like List, Dict, etc.
            typ = get_origin(annotation)
        else:
            typ = annotation

        if isinstance(typ, type):
            if typ is Parameter.empty:
                return "__unknown__"
            else:
                return typ.__name__
        return "__unknown__"

    def string_to_typehint(string_hint, globalns=None, localns=None):
        """Converts a string type hint to a valid type hint object.

        Args:
            string_hint (str): The string representation of the type hint.
            globalns (dict, optional): Global namespace dictionary. Defaults to None.
            localns (dict, optional): Local namespace dictionary. Defaults to None.

        Returns:
            type: The type hint object corresponding to the input string hint,
                or "__unknown__" if the type hint is not valid.
        """
        from typing import ForwardRef, _eval_type

        try:
            return _eval_type(ForwardRef(string_hint), globalns, localns)
        except NameError:
            return "__unknown__"

    def get_signature_as_dict(signature):
        """Return a dictionary containing information about the parameters of a function signature.

        Args:
            signature: A function signature object.

        Returns:
            dict: A dictionary where keys are parameter names and values
                are dictionaries containing the following information:
                - "typehint": The type hint of the parameter.
                - "default_value": The default value of the parameter (or "__empty__" if no default value is specified).
                - "kind": The kind of the parameter (e.g., POSITIONAL_ONLY, KEYWORD_ONLY, etc.).
        """
        from inspect import Parameter

        parameters = signature.parameters
        parsed_args = {}
        for name, param in parameters.items():

            parsed_args[name] = {
                "typehint": get_type_name(param.annotation),
                "default_value": param.default if param.default is not Parameter.empty else "__empty__",
                "kind": param.kind.name,
            }

        return parsed_args

    class Handshake(Task):
        name = f"{app_name}.handshake"

        def run(self):
            return f"{node()} is happy to shake your hand and says hello !"

    class TasksInfos(Task):
        name = f"{app_name}.tasks_infos"

        def run(self, app_name, selfish=False):
            """Run the specified app to gather tasks information.

            Args:
                app_name (str): The name of the app to run.
                selfish (bool, optional): Flag to indicate whether to include selfish tasks. Defaults to False.

            Returns:
                dict: A dictionary containing tasks information for the specified app.
            """
            app = APPLICATIONS_STORE[app_name]
            tasks_dynamic_data = {}
            pipelines = getattr(app, "pipelines", {})
            if len(pipelines) == 0:
                logger.warning(
                    "No pipeline is registered on this app instance. "
                    "Are you trying to read tasks infos from a non worker app ? (web server side ?)"
                )
                return {}
            for pipeline in pipelines.values():
                pipeline.resolve()
                for pipe in pipeline.pipes.values():
                    for step in pipe.steps.values():
                        if step.complete_name in app.tasks.keys():
                            str_sig = get_signature_as_string(step.generate.__signature__)
                            dict_sig = get_signature_as_dict(step.generate.__signature__)
                            doc = step.generate.__doc__
                            task_data = {
                                "signature": str_sig,
                                "signature_dict": dict_sig,
                                "docstring": doc,
                                "step_name": step.step_name,
                                "pipe_name": step.pipe_name,
                                "pipeline_name": step.pipeline_name,
                                "requires": [item.complete_name for item in step.requires],
                                "step_level_in_pipe": step.get_level(selfish=selfish),
                            }
                            tasks_dynamic_data[step.complete_name] = task_data
            return tasks_dynamic_data

    def get_remote_tasks(self):
        """Retrieve information about remote tasks.

        Returns:
            dict: A dictionary containing information about remote tasks, including workers and task names.
        """
        try:
            registered_tasks = self.control.inspect().registered_tasks()
        except ConnectionResetError:
            return None
        workers = []
        task_names = []
        if registered_tasks:
            for worker, tasks in registered_tasks.items():
                workers.append(worker)
                for task in tasks:
                    task_names.append(task)

        return {"workers": workers, "task_names": task_names}

    def get_celery_app_tasks(
        self, refresh=False, auto_refresh=3600 * 24, failed_refresh=60 * 5, initial_timeout=10, refresh_timeout=2
    ):
        """Get the celery app tasks data with optional refresh mechanism.

        Args:
            refresh (bool): Flag to force refresh the tasks data. Default is False.
            auto_refresh (int): Time interval in seconds for auto refresh. Default is 3600 seconds (1 hour).
            failed_refresh (int): Time interval in seconds for retrying refresh after failure.
                Default is 300 seconds (5 minutes).
            initial_timeout (int): Timeout in seconds for initial task data retrieval. Default is 10 seconds.
            refresh_timeout (int): Timeout in seconds for refreshing task data. Default is 2 seconds.

        Returns:
            dict: The task data of the celery app if available, otherwise None.
        """

        from datetime import datetime, timedelta

        auto_refresh_time = timedelta(0, seconds=auto_refresh)  # a full day (24 hours of 3600 seconds)
        failed_refresh_retry_time = timedelta(0, failed_refresh)  # try to refresh after 5 minutes

        app_task_data = getattr(self, "task_data", None)

        if app_task_data is None:
            try:
                task_data = self.tasks[f"{app_name}.tasks_infos"].delay(app_name).get(timeout=initial_timeout)
                # we set timeout to 10 sec if the task data doesn't exist.
                # It's long to wait for a webpage to load, but sometimes the workers take time to come out of sleep
                app_task_data = {"task_data": task_data, "refresh_time": datetime.now() + auto_refresh_time}
                setattr(self, "task_data", app_task_data)
                logger.warning("Got tasks data for the first time since django server relaunched")
            except Exception as e:
                logger.warning(f"Could not get tasks from app. {e}")
                # logger.warning(f"Remote tasks are : {self.get_remote_tasks()}")
                # logger.warning(f"Local tasks are : {self.tasks}")

        else:
            now = datetime.now()
            if now > app_task_data["refresh_time"]:  # we refresh if refresh time is elapsed
                logger.warning(
                    "Time has come to auto refresh app_task_data. "
                    f"refresh_time was {app_task_data['refresh_time']} and now is {now}"
                )
                refresh = True

            if refresh:
                try:
                    task_data = self.tasks[f"{app_name}.tasks_infos"].delay(app_name).get(timeout=refresh_timeout)
                    # if the data needs to be refreshed, we don't wait for as long as for a first get of infos.
                    app_task_data = {"task_data": task_data, "refresh_time": now + auto_refresh_time}
                    logger.warning("Refreshed celery tasks data sucessfully")
                except Exception as e:
                    logger.warning(
                        "Could not refresh tasks data from remote celery worker. All workers are is probably running. "
                        f"{e}"
                    )
                    app_task_data["refresh_time"] = now + failed_refresh_retry_time
                setattr(self, "task_data", app_task_data)
            else:
                delta = (app_task_data["refresh_time"] - now).total_seconds()
                logger.warning(f"Returned cached task_data. Next refresh will happen in at least {delta} seconds")
        return app_task_data["task_data"] if app_task_data is not None else None

    def launch_named_task_remotely(self, session_id, task_name, task_model=None, extra=None, kwargs={}):
        """Launches a named task remotely.

        Args:
            session_id (str): The session ID for the task.
            task_name (str): The name of the task to be launched.
            task_model (object, optional): The task model object. Defaults to None.
            extra (dict, optional): Extra data to be passed to the task. Defaults to None.
            kwargs (dict, optional): Additional keyword arguments to be passed to the task. Defaults to {}.

        Returns:
            CeleryTaskRecord: The task record created for the launched task.
        """

        if task_model is None:
            task_record = CeleryTaskRecord.create_from_task_name(
                self, task_name, app_name, session_id, extra=extra, **kwargs
            )
        else:
            task_record = CeleryTaskRecord.create_from_model(
                self, task_model, task_name, app_name, session_id, extra=extra, **kwargs
            )

        return task_record

    def is_hand_shaken(self):
        """Check if a handshake is successful.

        Returns:
            bool: True if handshake is successful, False otherwise.
        """
        try:
            result = self.tasks[f"{app_name}.handshake"].delay().get(timeout=1)
            logger.warning(f"Handshake result : {result}")
            return True
        except Exception as e:
            logger.error(f"No handshake result. All workers are busy ? {e}")
            return False

    def single_worker_start(self: "Celery"):
        thread = CeleryWorkerThread(self)
        thread.start()

    settings_files = get_setting_files_path(conf_path)

    if len(settings_files) == 0:
        logger.warning(f"{failure_message} Could not find celery toml config files.")
        return None

    try:
        from dynaconf import Dynaconf
    except ImportError:
        logger.warning(f"{failure_message} Could not import dynaconf. Maybe it is not istalled ?")
        return None

    try:
        settings = Dynaconf(settings_files=settings_files)
    except Exception as e:
        logger.warning(f"{failure_message} Could not create dynaconf object. {e}")
        return None

    try:
        app_display_name = settings.get("app_display_name", app_name)
        broker_type = settings.connexion.broker_type
        account = settings.account
        password = settings.password
        address = settings.address
        backend = settings.connexion.backend
        conf_data = settings.conf
        v_host = settings.broker_conf.virtual_host if v_host is None else v_host
    except (AttributeError, KeyError) as e:
        logger.warning(f"{failure_message} {e}")
        return None

    try:
        from celery import Celery
    except ImportError:
        logger.warning(f"{failure_message} Could not import celery. Maybe is is not installed ?")
        return None

    try:
        app = Celery(
            app_display_name,
            broker=f"{broker_type}://{account}:{password}@{address}/{v_host}",
            backend=f"{backend}://",
        )
    except Exception as e:
        logger.warning(f"{failure_message} Could not create app. Maybe rabbitmq server @{address} is not running ? {e}")
        return None

    for key, value in conf_data.items():
        try:
            setattr(app.conf, key, value)
        except Exception as e:
            logger.warning(f"{failure_message} Could assign extra attribute {key} to celery app. {e}")
            return None

    app.register_task(Handshake)
    app.register_task(TasksInfos)

    app.get_remote_tasks = MethodType(get_remote_tasks, app)  # type: ignore
    app.get_celery_app_tasks = MethodType(get_celery_app_tasks, app)  # type: ignore
    app.launch_named_task_remotely = MethodType(launch_named_task_remotely, app)  # type: ignore
    app.is_hand_shaken = MethodType(is_hand_shaken, app)  # type: ignore
    app.single_worker_start = MethodType(single_worker_start, app)

    logger.info(f"The celery app {app_name} was created successfully.")

    APPLICATIONS_STORE[app_name] = app

    return app


class CeleryWorkerThread(Thread):
    def __init__(self, app: "Celery"):
        super().__init__()
        self.app = app

    def run(self):
        self.app.worker_main(argv=["worker", "--loglevel=INFO", "--concurrency=1", "--pool=solo"])
        # self.app.start()

    def stop(self):
        worker_stats = self.app.control.inspect().stats()
        worker_names = worker_stats.keys() if worker_stats else []
        current_node_name = f"celery@{platform.node()}"
        if current_node_name in worker_names:
            self.app.control.broadcast("shutdown", destination=[current_node_name])
