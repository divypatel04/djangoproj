import time
from django.core.management.base import BaseCommand
from django.db.models import Count
from storage.models import FileChunk, Node, ChunkDistribution
from storage.views import ensure_chunk_replication, DISTRIBUTION_FACTOR
import requests

class Command(BaseCommand):
    help = 'Checks system health and fixes distribution issues'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Report issues without fixing them',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']

        self.stdout.write(self.style.NOTICE("Starting system health check..."))
        start_time = time.time()

        # Check node health
        nodes_count = self.check_nodes(dry_run)

        # Check chunk distribution
        chunks_fixed = self.check_distributions(dry_run)

        elapsed_time = time.time() - start_time
        self.stdout.write(self.style.SUCCESS(
            f"System health check completed in {elapsed_time:.2f} seconds.\n"
            f"Nodes checked: {nodes_count['total']}, reactivated: {nodes_count['reactivated']}\n"
            f"Chunks fixed: {chunks_fixed['fixed']} of {chunks_fixed['total']} needed repair"
        ))

    def check_nodes(self, dry_run):
        """Check all nodes and attempt to reactivate inactive ones."""
        self.stdout.write("Checking node health...")

        nodes = Node.objects.all()
        total_nodes = nodes.count()
        reactivated = 0

        inactive_nodes = nodes.filter(is_active=False)
        self.stdout.write(f"Found {inactive_nodes.count()} inactive nodes")

        for node in inactive_nodes:
            try:
                self.stdout.write(f"Testing node: {node.name} ({node.ip})...")
                response = requests.post(f"{node.get_api_url()}/id", timeout=5)

                if response.status_code == 200:
                    if not dry_run:
                        node.is_active = True
                        node.consecutive_failures = 0
                        node.save()
                        reactivated += 1
                        self.stdout.write(self.style.SUCCESS(f"  Reactivated node {node.name}"))
                    else:
                        self.stdout.write(self.style.SUCCESS(f"  Node {node.name} is online (would reactivate)"))
                else:
                    self.stdout.write(self.style.WARNING(
                        f"  Node {node.name} responded with status {response.status_code}"
                    ))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"  Failed to connect to {node.name}: {str(e)}"))

        return {
            'total': total_nodes,
            'reactivated': reactivated
        }

    def check_distributions(self, dry_run):
        """Check chunk distributions and fix under-replicated chunks."""
        self.stdout.write("Checking chunk distributions...")

        # Find under-distributed chunks
        under_distributed = FileChunk.objects.annotate(
            distribution_count=Count('distributions')
        ).filter(distribution_count__lt=DISTRIBUTION_FACTOR)

        total_under_distributed = under_distributed.count()
        self.stdout.write(f"Found {total_under_distributed} under-distributed chunks")

        fixed_count = 0
        for chunk in under_distributed:
            self.stdout.write(f"Chunk {chunk.ipfs_hash} has {chunk.distributions.count()} distributions")

            if not dry_run:
                success, message = ensure_chunk_replication(chunk, DISTRIBUTION_FACTOR)
                if success:
                    fixed_count += 1
                    self.stdout.write(self.style.SUCCESS(f"  Fixed: {message}"))
                else:
                    self.stdout.write(self.style.ERROR(f"  Failed: {message}"))
            else:
                self.stdout.write(self.style.WARNING(f"  Would attempt to fix (dry run)"))

        return {
            'total': total_under_distributed,
            'fixed': fixed_count
        }
