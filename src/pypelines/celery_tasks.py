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

from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from celery import Celery
    from .steps import BaseStep


APPLICATIONS_STORE = {}


class CeleryAlyxTaskManager(BaseStepTaskManager):

    backend: "CeleryTaskBackend"
    step: "BaseStep"

    def register_step(self):
        if self.backend:
            # self.backend.app.task(CeleryRunner, name=self.step.complete_name)
            self.backend.app.register_task(self.get_runner())

    def start(self, session, extra=None, **kwargs):

        if not self.backend:
            raise NotImplementedError(
                "Cannot start a task on a celery cluster as this pipeline " "doesn't have a working celery backend"
            )

        return CeleryTaskRecord.create(self, session, extra, **kwargs)

    def get_runner(superself):  # type: ignore
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

        if not task_infos_dict:
            from one import ONE

            connector = ONE(mode="remote", data_access_mode="remote")
            task_infos_dict = connector.alyx.rest("tasks", "read", id=task_id)

        super().__init__(task_infos_dict)
        self.session = session  # type: ignore
        self.response = response_handle

    def status_from_logs(self, log_object):
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
        from one import ONE

        connector = ONE(mode="remote", data_access_mode="remote")
        connector.alyx.rest("tasks", "partial_update", **self.export())

    def get_session(self):
        if self.session is None:
            from one import ONE

            connector = ONE(mode="remote", data_access_mode="remote")
            session = connector.search(id=self["session"], no_cache=True, details=True)
            self.session = session  # type: ignore

        return self.session

    def get_application(self):
        try:
            return APPLICATIONS_STORE[self["executable"]]
        except KeyError:
            raise KeyError(f"Unable to retrieve the application {self['executable']}")

    @property
    def pipeline_name(self):
        return self["name"].split(".")[0]

    @property
    def pipe_name(self):
        return self["name"].split(".")[1]

    @property
    def step_name(self):
        return self["name"].split(".")[2]

    @property
    def arguments(self):
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
        return self.session["path"]

    @property
    def task_id(self):
        return self["id"]

    def export(self):
        return {"id": self["id"], "data": {k: v for k, v in self.items() if k not in ["id", "session_path"]}}

    @staticmethod
    def create(task_manager: CeleryAlyxTaskManager, session, extra=None, **kwargs):
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

        new_task = task_model(
            name=task_name, session=session, arguments=kwargs, status="Waiting", executable=pipeline_name
        )
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
        super().__init__(parent)
        self.parent = parent

        if app is not None:
            self.success = True
            self.app = app

            pipelines = getattr(self.app, "pipelines", {})
            pipelines[parent.pipeline_name] = parent
            self.app.pipelines = pipelines

    def start(self):
        self.app.start()

    def create_task_manager(self, step):
        task_manager = self.task_manager_class(step, self)
        task_manager.register_step()
        return task_manager


class CeleryPipeline(Pipeline):
    runner_backend_class = CeleryTaskBackend


class LogTask:
    def __init__(self, task_record: CeleryTaskRecord, username=None, level="LOAD"):
        self.path = Path(task_record.session_path) / "logs"
        self.username = username if username is not None else (node() if node() else "unknown")
        self.worker_pk = task_record.task_id
        self.task_name = task_record["name"]
        self.level = getattr(logging, level.upper())

    def __enter__(self):
        self.path.mkdir(exist_ok=True)
        self.logger = getLogger()
        self.set_handler()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove_handler()

    def set_handler(self):
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
        self.logger.removeHandler(self.logger.handlers[-1])


def create_celery_app(conf_path, app_name="pypelines", v_host=None) -> "Celery | None":

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
        params = [
            param_value for param_name, param_value in signature.parameters.items() if param_name not in ["session"]
        ]
        return str(signature.replace(parameters=params))[1:-1].replace(" *,", "")

    def get_type_name(annotation):
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
        from typing import ForwardRef, _eval_type

        try:
            return _eval_type(ForwardRef(string_hint), globalns, localns)
        except NameError:
            return "__unknown__"

    def get_signature_as_dict(signature):
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
        registered_tasks = self.control.inspect().registered_tasks()
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
        try:
            result = self.tasks[f"{app_name}.handshake"].delay().get(timeout=1)
            logger.warning(f"Handshake result : {result}")
            return True
        except Exception as e:
            logger.error(f"No handshake result. All workers are busy ? {e}")
            return False

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

    logger.info(f"The celery app {app_name} was created successfully.")

    APPLICATIONS_STORE[app_name] = app

    return app
