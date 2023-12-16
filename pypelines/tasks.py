import celery
from functools import wraps

# this is still pseudocode for now


def make_pipeline_as_tasks(app, pipeline):
    for pipe in pipeline.pipes:
        for step in pipe.steps:
            app.tasks.add(step.generate)


def wrap_task(task):
    @wraps(task)
    def wrapper(session, *args, **kwargs):
        from one import ONE

        connector = ONE(mode="remote", data_access_mode="remote")
        data = {"session": session.id, "name": task.full_name, "status": "Waiting"}
        task_record = connector.alyx.rest("create", "tasks", data=data)

        task(session, *args, **kwargs)

    return wrapper
