from .tasks import BaseTaskBackend, BaseStepTaskManager
from .pipelines import Pipeline
from pathlib import Path
from traceback import format_exc as format_traceback_exc
from logging import getLogger
from functools import wraps
from .loggs import LogTask

from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from celery import Celery
    from .steps import BaseStep


class CeleryAlyxTaskManager(BaseStepTaskManager):

    backend: "CeleryTaskBackend"
    step: "BaseStep"

    def register_step(self):
        if self.backend:
            self.backend.app.task(self.runner, name=self.step.complete_name)

    def runner(self, task_id, extra=None):

        from one import ONE

        connector = ONE(mode="remote", data_access_mode="remote")
        task = CeleryTaskRecord(task_id)

        try:
            session = connector.search(
                id=task["session"], details=True, no_cache=True)

            with LogTask(task) as log_object:
                logger = log_object.logger
                task["log"] = log_object.filename
                task["status"] = "Started"
                task.partial_update()

                try:
                    self.step.generate(
                        session, extra=extra, skip=True, check_requirements=True, **task.arguments)
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

    def start(self, session, extra=None, **kwargs):

        if not self.backend:
            raise NotImplementedError(
                "Cannot start a task on a celery cluster as this pipeline "
                "doesn't have a working celery backend"
            )

        return CeleryTaskRecord.create(self, session, extra, **kwargs)


class CeleryTaskRecord(dict):
    # a class to make dictionnary keys accessible with attribute syntax
    def __init__(self, task_id, task_infos_dict={}, response_handle=None):
        if task_infos_dict:
            super().__init__(task_infos_dict)
        else:
            from one import ONE

            connector = ONE(mode="remote", data_access_mode="remote")
            task_infos_dict = connector.alyx.rest("tasks", "read", id=task_id)
            super().__init__(task_infos_dict)

        self.session = 
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

    @property
    def arguments(self):
        args = self.get("arguments", {})
        return args if args else {}

    @property
    def session_path(self):
        from one import ONE
        connector = ONE(mode="remote", data_access_mode="remote")
        connector.alyx.rest(sess)

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
            "executable": task_manager.backend.app_name,
        }

        task_dict = connector.alyx.rest("tasks", "create", data=data)

        worker = task_manager.backend.app.tasks[task_manager.step.complete_name]
        response_handle = worker.delay(task_dict["id"], extra=extra)

        return TaskRecord(task_dict["id"], task_dict, response_handle)


class CeleryTaskBackend(BaseTaskBackend):
    app: "Celery"
    task_manager_class = CeleryAlyxTaskManager

    def __init__(self, parent: Pipeline, app: "Celery | None" = None):
        super().__init__(parent)
        self.parent = parent

        if app is not None:
            self.success = True
            self.app = app

    def start(self):
        self.app.start()

    def create_task_manager(self, step):
        task_manager = self.task_manager_class(step, self)
        task_manager.register_step()
        return task_manager


class CeleryPipeline(Pipeline):
    runner_backend_class = CeleryTaskBackend


def get_setting_files_path(conf_path, app_name) -> List[Path]:
    conf_path = Path(conf_path)
    if conf_path.is_file():
        conf_path = conf_path.parent
    files = []
    for prefix, suffix in zip(["", "."], ["", "_secrets"]):
        file_loc = conf_path / f"{prefix}celery_{app_name}{suffix}.toml"
        if file_loc.is_file():
            files.append(file_loc)
    return files


class LogTask:
    def __init__(self, task_record, username="", level="LOAD"):
        self.path = os.path.normpath(task_record.session_path)
        self.username = username
        os.makedirs(self.path, exist_ok=True)
        self.worker_pk = task_record.id
        self.task_name = task_record.name
        self.level = getattr(logging, level.upper())

    def __enter__(self):
        self.logger = logging.getLogger()
        self.set_handler()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove_handler()

    def set_handler(self):
        self.filename = os.path.join(
            "logs", f"task_log.{self.task_name}.{self.worker_pk}.log")
        self.fullpath = os.path.join(self.path, self.filename)
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
        fh.setFormatter()
        self.logger.addHandler(fh)

    def remove_handler(self):
        self.logger.removeHandler(self.logger.handlers[-1])


def create_celery_app(conf_path, app_name="pypelines"):

    failure_message = (
        f"Celery app : {app_name} failed to be created."
        "Don't worry, about this alert, "
        "this is not be an issue if you didn't explicitely planned on using celery. Issue was : "
    )

    logger = getLogger("pypelines.create_celery_app")
    settings_files = get_setting_files_path(conf_path, app_name)

    if len(settings_files) == 0:
        logger.warning(
            f"{failure_message} Could not find celery toml config files.")
        return None

    try:
        from dynaconf import Dynaconf
    except ImportError:
        logger.warning(
            f"{failure_message} Could not import dynaconf. Maybe it is not istalled ?")
        return None

    try:
        settings = Dynaconf(settings_files=settings_files)
    except Exception as e:
        logger.warning(
            f"{failure_message} Could not create dynaconf object. {e}")
        return None

    try:
        app_display_name = settings.get("app_display_name", app_name)
        broker_type = settings.connexion.broker_type
        account = settings.account
        password = settings.password
        address = settings.address
        backend = settings.connexion.backend
        conf_data = settings.conf
    except (AttributeError, KeyError) as e:
        logger.warning(f"{failure_message} {e}")
        return None

    try:
        from celery import Celery
    except ImportError:
        logger.warning(
            f"{failure_message} Could not import celery. Maybe is is not installed ?")
        return None

    try:
        app = Celery(
            app_display_name,
            broker=(f"{broker_type}://" f"{account}:{password}@{address}//"),
            backend=f"{backend}://",
        )
    except Exception as e:
        logger.warning(
            f"{failure_message} Could not create app. Maybe rabbitmq server @{address} is not running ? {e}")
        return None

    for key, value in conf_data.items():
        try:
            setattr(app.conf, key, value)
        except Exception as e:
            logger.warning(
                f"{failure_message} Could assign extra attribute {key} to celery app. {e}")
            return None

    return app
