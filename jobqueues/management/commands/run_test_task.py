import json
from datetime import datetime, timedelta
from time import sleep

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from ...tasks import TestTask
from ...mq import TaskQueueConnection
from ...models import ScheduledJob

class Command(BaseCommand):
    help = 'Test the task processing infrastructure'

    # TODO: Make these arguments
    MAX_WAIT = timedelta(minutes=2)

    def handle(self, *args, **options):
        broker = TaskQueueConnection()
        task = TestTask()

        self.stdout.write("Sending Test Task")
        job = broker.schedule(task)
        self.stdout.write("Scheduled task #%d" % (job.id))

        self.stdout.write("Waiting for task to complete")
        started = datetime.now()
        running = False
        while True:

            sleep(2)
            if datetime.now() - started > self.MAX_WAIT:
                self.stdout.write(self.style.ERROR("Giving up on job after waiting for " + (str(datetime.now() - started))))
                return

            job.refresh_from_db()

            if job.status == ScheduledJob.RUNNING:
                if not running:
                    self.stdout.write("Job is running")
                    running = True
                continue

            elif job.status == ScheduledJob.SUCCESS:
                self.stdout.write(self.style.SUCCESS("Job Finished"))
                return

            elif job.status == ScheduledJob.ERROR:
                self.stdout.write(self.style.ERROR("Job Failed"))
                return

            elif job.status != ScheduledJob.PENDING:
                self.stdout.write(self.style.ERROR("Unknown job status: " + str(job.status)))
                return


