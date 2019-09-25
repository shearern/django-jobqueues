from threading import Lock

import pika

from django.conf import settings

from .models import ScheduledJob


class TaskQueueConnection:

    def __init__(self):
        self.conn = None
        self.lock = Lock()


    @property
    def broker_url(self):
        try:
            return settings.TASK_BROKER_URL
        except AttributeError:
            raise Exception("Missing TASK_BROKER_URL in django setting")


    @property
    def queue_name(self):
        '''Name of the queue in the broker'''
        try:
            return settings.TASK_QUEUE_NAME
        except AttributeError:
            return 'django_tasks'

    
    def mq_channel(self):

        with self.lock:
            if self.conn is None:
                self.conn = pika.BlockingConnection(pika.ConnectionParameters(self.broker_url))

            channel = self.conn.channel()

            # Declare queue (idempotent)
            channel.queue_declare(
                queue=self.queue_name,
                durable=True,  # Preserve messages on MQ reboot
            )

            return channel


    def schedule(self, task):
        '''
        Schedule a task to be executed by the worker processes
    
        The input is passed just to the message broker for efficiency.  The worker
        will eventually record the input data into the ProcessingTaskResults record.

        :param task: Task to schedule
        '''

        job = ScheduledJob(
            task_class = task.task_class,
            status = ScheduledJob.PENDING,
            summary = task.summary,
        )
        job.input_data = task.init_parms
        job.save()

        # Send to broker to notify worker
        self._send_to_broker(job.id)
        # If fails, a processor may pick up task when restarted.

        return job

    
    def _send_to_broker(self, job_id):

        channel = self.mq_channel()

        # Send task message
        channel.basic_publish(
            exchange = '', # default exchange I believe
            routing_key = self.queue_name,
            body = str(job_id),
            properties = pika.BasicProperties(
                delivery_mode = 2, # make message persist MQ reboots
            )
        )
    
        # Close and flush message
        channel.close()


class TaskQueueBrokerListener(TaskQueueConnection):

    def __init__(self, callback):

        super(TaskQueueBrokerListener, self).__init__()

        self.__channel = self.mq_channel()
        self.__channel.basic_qos(prefetch_count=1) # Only queue one task at a time for this worker (no round-robbin)

        # Setup monitor and callback loop
        self.__channel.basic_consume(queue=settings.TASK_QUEUE_NAME, on_message_callback = callback)


    def start_listening(self):
        self.__channel.start_consuming()
