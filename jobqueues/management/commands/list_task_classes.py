
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from ...TaskFactory import find_task_classes

class Command(BaseCommand):
    help = 'Closes the specified poll for voting'

    def handle(self, *args, **options):
        cnt = 0
        for module_path, task_class in find_task_classes():
            self.stdout.write(module_path)
            cnt += 1
        self.stdout.write("Found %d task classes" % (cnt))

