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
    success: bool = False

    def __init__(self, conf_path, pipeline_name):
        logger = getLogger()
        settings_files = self.get_setting_files_path(conf_path, pipeline_name)

        if any([not os.path.isfile(file) for file in settings_files]):
            logger.warning(f"Some celery configuration files were missing for pipeline {pipeline_name}")
            return

        try:
            self.settings = Dynaconf(settings_files=settings_files)

            self.app_name = self.settings.get("app_name", pipeline_name)
            broker_type = self.settings.connexion.broker_type
            account = self.settings.account
            password = self.settings.password
            address = self.settings.address
            backend = self.settings.connexion.backend
            conf_data = self.settings.conf

        except Exception as e:
            logger.warning(
                "Could not get all necessary information to configure celery when reading config files."
                "Check their content."
            )
            return

        try:
            self.app = Celery(
                self.app_name,
                broker=(f"{broker_type}://" f"{account}:{password}@{address}//"),
                backend=f"{backend}://",
            )
        except Exception as e:
            logger.warning("Instanciating celery app failed. Maybe rabbitmq is not running ?")

        for key, value in conf_data.items():
            setattr(self.app.conf, key, value)

        try:
            self.connector = ONE(data_access_mode="remote")
        except Exception as e:
            logger.warning("Instanciating One connector during celery configuration failed.")
            return

        self.success = True

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
