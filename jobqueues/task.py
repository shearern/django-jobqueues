'''
Base class and supporting methods to schedule and run tasks.

Goal is:

 1) MySpecialTask.create(parms).schedule()
    -> Creates ProcessingTaskLog record
    -> Submits task_id to MQ

 2) manage.py process_mq_tasks
    -> Picks up task_id from MQ
    -> Loads ProcessingTaskLog record
    -> Re-creates MySpecialTask(task)
    -> task.execute()

'''

from abc import ABC, abstractmethod


from .models import ScheduledJob

# class WorkerRegistry(type):
#     '''
#     Captures list of implemented classes
#     ref: https://stackoverflow.com/questions/3507125/how-can-i-discover-classes-in-a-specific-package-in-python
#     '''
#
#     def __init__(cls, name, bases, cls_dict):
#         type.__init__(cls, name, bases, cls_dict)
#         cls._subclasses = set()
#         for base in bases:
#             if isinstance(base,WorkerRegistry):
#                 base._subclasses.add(cls)
#
#     def all_tasks(cls):
#         return reduce( set.union,
#                        ( succ.all_tasks() for succ  in cls._subclasses if isinstance(succ,WorkerRegistry)),
#                        cls._subclasses)
#

class Task(ABC):
    '''Base class to implement a worker to complete tasks'''


    def __init__(self):
        self.scheduled_task = None
        self.__initialized = True


    def _assert_initialized(self):
        try:
            self.__initialized
        except AttributeError:
            raise Exception("Task.__init__() never called for %s" % (self.__class__.__name___))


    def attach_scheduled_task(self, record):
        self._assert_initialized()
        self.scheduled_task = record


    @property
    def task_class(self):
        return self.__class__.__name__


    @property
    @abstractmethod
    def init_parms(self):
        '''Parameters to recreate this task class'''


    @property
    @abstractmethod
    def summary(self):
        '''Create a user friendly summary of the task'''
        raise NotImplementedError("%s.summary() not implemented" % (self.__class__.__name__))


    @abstractmethod
    def execute(self):
        '''
        Perform the action for the given task

        :param task: ProcessingTask record
        '''
        raise NotImplementedError("%s.execute() not implemented" % (self.__class__.__name__))


