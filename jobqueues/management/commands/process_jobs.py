import json
from datetime import datetime
from time import sleep
import logging

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from ...mq import TaskQueueBrokerListener
from ...models import ScheduledJob
from ...TaskFactory import rebuild_task


class Command(BaseCommand):
    help = 'Closes the specified poll for voting'

    def handle(self, *args, **options):

        # Connect to MQ
        self.stdout.write("Connecting to " + settings.TASK_BROKER_URL)
        mq = TaskQueueBrokerListener(callback=self._receive_mq_msg)

        self.stdout.write("Waiting for tasks")
        mq.start_listening()


    def _receive_mq_msg(self, channel, method, properties, body):
        '''
        Handle task messages received from MQ
        '''

        job = None
        try:

            # Interpret message body
            try:
                job_id = int(body.decode('utf-8'))
            except Exception as e:
                self.stdout.write(self.style.ERROR("Couldn't decode task body: %s" % (str(e))))
                return

            # Retrieve processing task
            try:
                job = ScheduledJob.objects.get(id=int(body))
            except ScheduledJob.ObjectDoesNotExist:
                self.stdout.write(self.style.ERROR("Job %d doesn't exist: " % (job_id)))
                return
            except Exception as e:
                self.stdout.write(self.style.ERROR("Failed to retrieve job %s: %s" (job_id, str(e))))
                return

            try:

                # Make sure task not already started
                if job.status != ScheduledJob.PENDING:
                    raise Exception("%s job #%d already in status %s" % (
                        job.task_class, job.id, job.status))

                # Task start accounting
                job.started = datetime.now()
                job.status = ScheduledJob.RUNNING
                job.save()

                # Handle task
                try:
                    task = rebuild_task(job)
                except Exception as e:
                    raise Exception("ERROR Rebuilding Task: " + str(e))

                try:
                    self.stdout.write("Starting %s task %d" % (job.task_class, job.id))
                    task.execute()
                    self.stdout.write("Finished %s task %d" % (job.task_class, job.id))
                except Exception as e:
                    # TODO: capture call stack
                    raise Exception("%s while performing %s task %s: %s" % (
                        e.__class__.__name__, job.task_class, job.summary, str(e)))

                # Make sure task is done
                if job.status == job.PENDING:
                    job.status = job.SUCCESS

                # Mark execution time
                job.status = ScheduledJob.SUCCESS
                job.ended = datetime.now()
                job.duration = (job.ended - job.started).total_seconds()

                job.save()

            except Exception as e:
                job.status = ScheduledJob.ERROR
                job.result = {
                    'error': str(e)
                }
                job.save()
                self.stdout.write(self.style.ERROR("%s job %d failed: %s" % (job.task_class, job.id, str(e))))

        finally:
            # Ack message to get another and prevent re-delivery
            channel.basic_ack(delivery_tag=method.delivery_tag)




