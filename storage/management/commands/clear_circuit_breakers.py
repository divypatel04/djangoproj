import time
from django.core.management.base import BaseCommand
from storage.views import NODE_FAILURE_CACHE
from storage.models import Node

class Command(BaseCommand):
    help = 'Clears circuit breaker locks for nodes'

    def add_arguments(self, parser):
        parser.add_argument(
            '--node-id',
            type=int,
            help='Reset circuit breaker for a specific node ID',
        )

        parser.add_argument(
            '--list',
            action='store_true',
            help='List current circuit breaker status without clearing',
        )

    def handle(self, *args, **options):
        node_id = options.get('node_id')
        list_only = options.get('list')

        if list_only:
            # Just list the current status
            self.list_circuit_breakers()
            return

        if node_id:
            # Clear for a specific node
            self.clear_for_node(node_id)
        else:
            # Clear all circuit breakers
            self.clear_all_circuit_breakers()

    def list_circuit_breakers(self):
        """Lists the current circuit breaker entries."""
        self.stdout.write(self.style.NOTICE("Current Circuit Breaker Status:"))

        if not NODE_FAILURE_CACHE:
            self.stdout.write("No active circuit breaker entries")
            return

        self.stdout.write(f"Total entries: {len(NODE_FAILURE_CACHE)}")
        self.stdout.write("-" * 80)

        for key, data in NODE_FAILURE_CACHE.items():
            node_id, operation = key.split(':') if ':' in key else (key, 'unknown')
            node_name = data.get('node_name', 'Unknown')
            failures = data.get('count', 0)
            timestamp = data.get('timestamp', 'Unknown')

            self.stdout.write(
                f"Node: {node_name} (ID: {node_id})\n"
                f"Operation: {operation}\n"
                f"Failures: {failures}\n"
                f"Last failure: {timestamp}\n"
                f"-" * 40
            )

    def clear_for_node(self, node_id):
        """Clears circuit breaker entries for a specific node."""
        node = None
        try:
            node = Node.objects.get(id=node_id)
            self.stdout.write(f"Clearing circuit breaker for node: {node.name} (ID: {node_id})")
        except Node.DoesNotExist:
            self.stdout.write(self.style.WARNING(f"Node with ID {node_id} not found in database"))

        keys_to_delete = []
        for key in NODE_FAILURE_CACHE:
            if key.startswith(f"{node_id}:"):
                keys_to_delete.append(key)

        if not keys_to_delete:
            self.stdout.write(self.style.SUCCESS(f"No circuit breaker entries found for node ID {node_id}"))
            return

        # Delete the keys
        for key in keys_to_delete:
            del NODE_FAILURE_CACHE[key]

        self.stdout.write(self.style.SUCCESS(
            f"Cleared {len(keys_to_delete)} circuit breaker entries for node ID {node_id}"
        ))

    def clear_all_circuit_breakers(self):
        """Clears all circuit breaker entries."""
        entry_count = len(NODE_FAILURE_CACHE)

        if entry_count == 0:
            self.stdout.write(self.style.SUCCESS("No circuit breaker entries to clear"))
            return

        # Clear the cache
        NODE_FAILURE_CACHE.clear()

        self.stdout.write(self.style.SUCCESS(f"Cleared {entry_count} circuit breaker entries"))
