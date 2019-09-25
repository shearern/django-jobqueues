from datetime import datetime
from time import sleep

from ..task import Task

class TestTask(Task):
    '''A simple task to test whether the task infrastructure is working'''

    def __init__(self, ts=None):
        self.ts = ts
        if self.ts is None:
            self.ts = str(datetime.now())

    @property
    def init_parms(self):
        return {
            'ts': self.ts,
        }

    @property
    def summary(self):
        return "Test task started at " + self.ts

    def execute(self):
        sleep(2)


