import inspect
import pkgutil
from importlib import import_module

from django import apps

from .task import Task


def find_task_classes():
    '''Find task classes defined in loaded Django applications'''

    for app in apps.apps.app_configs.values():
        if inspect.ismodule(app.module):

            # Check for '(app).tasks' module
            app_tasks_module_name = app.name + '.tasks'
            try:
                tasks_module = import_module(app_tasks_module_name)
                for name, member in inspect.getmembers(tasks_module):
                    if not name.startswith('_') and inspect.isclass(member):
                        yield '%s.%s' % (app_tasks_module_name, name), member
            except ModuleNotFoundError:
                pass


ALL_TASK_CLASSES = {c.__name__: c for (n, c) in find_task_classes()}
def rebuild_task(scheduled_job):

    # Get task class
    try:
        cls = ALL_TASK_CLASSES[scheduled_job.task_class]
    except KeyError:
        raise NameError("No task class for %s" % (scheduled_job.task_class))

    # Get parameters to recreate task
    try:
        init_parms = scheduled_job.input_data
    except Exception as e:
        raise ValueError("Failed to load init parms:\n%s\n" % (repr(scheduled_job.job_data_json, str(e))))

    # Re-instantiate task
    return cls(**init_parms)