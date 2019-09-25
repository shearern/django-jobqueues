import json

from django.db.models import *

class ScheduledJob(Model):
    '''A resource that can be queried from Ellucian'''

    PENDING = 'P'
    RUNNING = 'R'
    SUCCESS = 's'
    ERROR = 'e'

    STATUS_CODES = {
        PENDING: 'Pending',
        RUNNING: "Running",
        SUCCESS: "Success",
        ERROR: "Failed",
    }

    # -- DB Fields ------------------------------------------------------------
    task_class = CharField(max_length=120)
    status = CharField(max_length=1, choices=STATUS_CODES.items(), default=PENDING)
    summary = CharField(max_length=128, blank=True, null=True)

    # Timing
    scheduled = DateTimeField(auto_now=True)
    started = DateTimeField(blank=True, null=True)
    ended = DateTimeField(blank=True, null=True)
    duration = FloatField(blank=True, null=True) # seconds

    # Job Data
    input_data_json = CharField(max_length=4096, blank=True, null=True)

    # Output
    result_json = CharField(max_length=4096, blank=True, null=True)

    # -------------------------------------------------------------------------

    def __init__(self, *args, **kwargs):
        if 'input_data' in kwargs:
            kwargs['input_data_json'] = json.dumps(kwargs['input_data'])
            del kwargs['input_data']
        super(ScheduledJob, self).__init__(*args, **kwargs)


    def __str__(self):
        return self.summary

    class Meta:
        ordering = ('-scheduled', )


    @property
    def input_data(self):
        if self.input_data_json is not None:
            return json.loads(self.input_data_json)

    @input_data.setter
    def input_data(self, value):
        self.input_data_json = json.dumps(value)


    @property
    def result(self):
        if self.result_json is not None:
            return json.loads(self.result_json)

    @result.setter
    def result(self, value):
        self.result_json = json.dumps(value)



    # -- Message Broker Methods -----------------------------------------------


