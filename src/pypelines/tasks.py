from functools import wraps
from logging import getLogger
import os, traceback

from celery import Celery
from dynaconf import Dynaconf
from one import ONE
from .loggs import LogTask


class CeleryHandler:
    settings = None
    app = None
    app_name = None

    def __init__(self, conf_path, pipeline_name):
        settings_files = self.get_setting_files_path(conf_path, pipeline_name)
        self.settings = Dynaconf(settings_files=settings_files)
        self.app_name = self.settings.get("app_name", pipeline_name)
        self.app = Celery(
            self.app_name,
            broker=(
                f"{self.settings.connexion.broker_type}://"
                f"{self.settings.account}:{self.settings.password}@{self.settings.address}//"
            ),
            backend=f"{self.settings.connexion.backend}://",
        )

        for key, value in self.settings.conf.items():
            setattr(self.app.conf, key, value)

        self.connector = ONE(data_access_mode="remote")

    def get_setting_files_path(self, conf_path, pipeline_name):
        files = []
        files.append(os.path.join(conf_path, f"celery_{pipeline_name}.toml"))
        files.append(os.path.join(conf_path, f".celery_{pipeline_name}_secrets.toml"))
        return files

    def register_step(self, step):
        self.app.task(self.wrap_step(step), name=step.full_name)

    def wrap_step(self, step):
        @wraps(step.generate)
        def wrapper(task_id, extra=None):  # session, *args, extra=None, **kwargs):
            from one import ONE

            connector = ONE(mode="remote", data_access_mode="remote")
            task = TaskRecord(connector.alyx.rest("tasks", "read", id=task_id))
            kwargs = task.arguments if task.arguments else {}

            try:
                session = connector.search(id=task.session, details=True)

                with LogTask(task) as log_object:
                    logger = log_object.logger
                    task.log = log_object.filename
                    task.status = "Started"
                    task = TaskRecord(connector.alyx.rest("tasks", "partial_update", **task.export()))

                    try:
                        step.generate(session, extra=extra, skip=True, check_requirements=True, **kwargs)
                        task.status = CeleryHandler.status_from_logs(log_object)
                    except Exception as e:
                        traceback_msg = traceback.format_exc()
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


class TaskRecord(dict):
    # a class to make dictionnary keys accessible with attribute syntax
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self

    def export(self):
        return {"id": self.id, "data": {k: v for k, v in self.items() if k not in ["id", "session_path"]}}
