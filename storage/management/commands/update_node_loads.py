from django.core.management.base import BaseCommand
from storage.models import Node

class Command(BaseCommand):
    help = 'Update the load percentage for all nodes based on capacity usage'

    def handle(self, *args, **kwargs):
        nodes = Node.objects.all()
        updated_count = 0

        for node in nodes:
            old_load = node.load
            node.update_load()
            self.stdout.write(f"Node {node.name}: Load updated from {old_load}% to {node.load}%")
            updated_count += 1

        self.stdout.write(self.style.SUCCESS(f'Successfully updated {updated_count} nodes'))
