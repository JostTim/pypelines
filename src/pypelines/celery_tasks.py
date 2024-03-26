from .tasks import BaseTaskBackend
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


class CeleryTaskBackend(BaseTaskBackend):
    app: "Celery"

    def __init__(self, parent: Pipeline, app: "Celery | None" = None):
        super().__init__(parent)
        self.parent = parent

        if app is not None:
            self.success = True
            self.app = app

    def start(self):
        self.app.start()

    def register_step(self, step: "BaseStep"):
        wrapped_step = getattr(step, "queue", None)
        if wrapped_step is None:
            return
        if self:
            self.app.task(wrapped_step, name=step.complete_name)

    def wrap_step(self, step):

        @wraps(step.generate)
        def wrapper(task_id, extra=None):  # session, *args, extra=None, **kwargs):
            from one import ONE

            connector = ONE(mode="remote", data_access_mode="remote")
            task = TaskRecord(connector.alyx.rest("tasks", "read", id=task_id))
            kwargs = task.arguments if task.arguments else {}

            try:
                session = connector.search(id=task.session, details=True, no_cache=True)

                with LogTask(task) as log_object:
                    logger = log_object.logger
                    task.log = log_object.filename
                    task.status = "Started"
                    task = TaskRecord(connector.alyx.rest("tasks", "partial_update", **task.export()))

                    try:
                        step.generate(session, extra=extra, skip=True, check_requirements=True, **kwargs)
                        task.status = CeleryTaskBackend.status_from_logs(log_object)
                    except Exception as e:
                        traceback_msg = format_traceback_exc()
                        logger.critical(f"Fatal Error : {e}")
                        logger.critical("Traceback :\n" + traceback_msg)
                        task.status = "Failed"

            except Exception as e:
                # if it fails outside of the nested try statement, we can't store logs files,
                # and we mention the failure through alyx directly.
                task.status = "Uncatched_Fail"
                task.log = str(e)

            connector.alyx.rest("tasks", "partial_update", **task.export())

        return wrapper

    @staticmethod
    def status_from_logs(log_object):
        with open(log_object.fullpath, "r") as f:
            content = f.read()

        if len(content) == 0:
            return "No_Info"
        if "CRITICAL" in content:
            return "Failed"
        elif "ERROR" in content:
            return "Errors"
        elif "WARNING" in content:
            return "Warnings"
        else:
            return "Complete"


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


def create_celery_app(conf_path, app_name="pypelines"):

    failure_message = (
        f"Celery app : {app_name} failed to be created."
        "Don't worry, about this alert, "
        "this is not be an issue if you didn't explicitely planned on using celery. Issue was : "
    )

    logger = getLogger("pypelines.create_celery_app")
    settings_files = get_setting_files_path(conf_path, app_name)

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
            broker=(f"{broker_type}://" f"{account}:{password}@{address}//"),
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

    return app


class TaskRecord(dict):
    # a class to make dictionnary keys accessible with attribute syntax
    def __init__(self, *args, **kwargs):
        from one import ONE

        self.connector = ONE(mode="remote", data_access_mode="remote")
        super().__init__(*args, **kwargs)
        self.__dict__ = self

    def export(self):
        return {"id": self["id"], "data": {k: v for k, v in self.items() if k not in ["id", "session_path"]}}

    @staticmethod
    def create(step: "BaseStep", session):
        from one import ONE

        connector = ONE(mode="remote", data_access_mode="remote")

        data = {"name": step.complete_name}
        connector.alyx.rest("tasks", "create", data=data)
