from django.db import models
from django.contrib.auth.models import User

# Import constants directly to avoid circular imports
BLACKLISTED_IPS = ['103.143.148.185', '1.36.226.78', '104.168.82.126']

class File(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    fname = models.CharField(max_length=255)  # Keep this as fname, not name
    ipfs_hash = models.CharField(max_length=255)
    size = models.BigIntegerField(null=True)
    uploaded_at = models.DateTimeField(auto_now_add=True)
    checksum = models.CharField(max_length=255)

    def __str__(self):
        return self.fname  # Use fname consistently

    def get_distribution_status(self):
        """Returns the distribution status of this file's chunks across nodes."""
        chunks = self.chunks.all()
        if not chunks:
            return {"distributed": False, "node_count": 0}

        total_distributions = 0
        nodes_used = set()

        for chunk in chunks:
            chunk_distributions = ChunkDistribution.objects.filter(chunk=chunk)
            total_distributions += chunk_distributions.count()
            for dist in chunk_distributions:
                nodes_used.add(dist.node.id)

        return {
            "distributed": len(nodes_used) > 1,
            "node_count": len(nodes_used),
            "distributions_per_chunk": total_distributions / chunks.count() if chunks.count() > 0 else 0
        }

class FileChunk(models.Model):
    file = models.ForeignKey(File, on_delete=models.CASCADE, related_name="chunks")
    chunk_index = models.IntegerField()
    ipfs_hash = models.CharField(max_length=255, unique=True)
    reference_hash = models.CharField(max_length=255, null=True, blank=True)
    size = models.BigIntegerField(null=True)

    def __str__(self):
        return f"Chunk {self.chunk_index} of {self.file.fname}"  # Use fname consistently

    def get_node_distributions(self):
        """Returns all nodes where this chunk is stored."""
        return self.distributions.all()

    def get_ipfs_access_hash(self):
        """Returns the hash to use for IPFS access (original hash if this is a reference)"""
        return self.reference_hash or self.ipfs_hash

class NodeManager(models.Manager):
    def get_queryset(self):
        """Filter out blacklisted IPs from all Node queries by default."""
        return super().get_queryset().exclude(ip__in=BLACKLISTED_IPS)

class Node(models.Model):
    """A node in the IPFS network."""
    name = models.CharField(max_length=100, unique=True)
    ipfs_id = models.CharField(max_length=100, unique=True)
    ip = models.CharField(max_length=100)
    port = models.IntegerField(default=5001)
    api_url = models.CharField(max_length=200, null=True, blank=True)
    is_active = models.BooleanField(default=True)
    load = models.FloatField(default=0.0)
    last_seen = models.DateTimeField(auto_now=True)
    consecutive_failures = models.IntegerField(default=0)

    # Use the custom manager for normal queries
    objects = NodeManager()

    # Include a manager that doesn't filter blacklisted IPs for admin operations
    all_objects = models.Manager()

    def __str__(self):
        return self.name

    def get_api_url(self):
        """Get the full API URL for this node."""
        if self.api_url:
            return self.api_url
        return f"http://{self.ip}:{self.port}/api/v0"

class ChunkDistribution(models.Model):
    """Tracks which chunks are stored on which nodes."""
    chunk = models.ForeignKey(FileChunk, on_delete=models.CASCADE, related_name="distributions")
    node = models.ForeignKey(Node, on_delete=models.CASCADE, related_name="chunk_distributions")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('chunk', 'node')  # Prevent duplicate distributions

    def __str__(self):
        return f"{self.chunk} on {self.node.name}"
