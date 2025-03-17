"""Utility functions for the storage application views."""

import json
import random
import time
import requests
from datetime import datetime, timedelta
from django.db.models import Count

from ..models import Node, ChunkDistribution
from ..constants import CHUNK_SIZE, IPFS_API_URL, TIMEOUTS, DISTRIBUTION_FACTOR

# Cache to store node failure information
NODE_FAILURE_CACHE = {}

def chunk_file(file):
    """Splits a file into chunks and returns the chunk index and data."""
    file.seek(0)  # Reset file pointer to beginning
    chunk_index = 0

    while True:
        chunk_data = file.read(CHUNK_SIZE)
        if not chunk_data:
            break
        yield (chunk_index, chunk_data)
        chunk_index += 1

def get_optimal_nodes(count=DISTRIBUTION_FACTOR, prefer_healthy=True):
    """Returns the optimal nodes for distributing chunks based on load and health."""
    # Get active nodes ordered by load (lowest load first)
    nodes_query = Node.objects.filter(is_active=True)

    # If we prefer healthy nodes (no recent failures), prioritize them
    if prefer_healthy:
        healthy_nodes = nodes_query.filter(consecutive_failures=0).order_by('load')
        if healthy_nodes.exists() and healthy_nodes.count() >= count:
            nodes = healthy_nodes
        else:
            # If not enough healthy nodes, get all active nodes
            nodes = nodes_query.order_by('load')
    else:
        nodes = nodes_query.order_by('load')

    if not nodes.exists():
        # If no nodes found, try to discover them
        discover_ipfs_nodes()
        nodes = Node.objects.filter(is_active=True).order_by('load')

        # If still no nodes, create a local node
        if not nodes.exists():
            local_node = Node.objects.create(
                name="LocalNode",
                ipfs_id="local",
                ip="127.0.0.1",
                load=0.0
            )
            nodes = [local_node]

    # If we have fewer nodes than requested, return all available
    if nodes.count() < count:
        return list(nodes)

    # Return a mix of least loaded nodes and some random ones for better distribution
    selected_nodes = list(nodes[:count-1])  # Get the least loaded nodes

    # Add one random node that wasn't already selected
    remaining_nodes = list(nodes.exclude(id__in=[n.id for n in selected_nodes]))
    if remaining_nodes:
        selected_nodes.append(random.choice(remaining_nodes))

    return selected_nodes

def should_skip_node(node_id, operation_type='default'):
    """Determines if a node should be skipped based on recent failures."""
    now = datetime.now()
    key = f"{node_id}:{operation_type}"

    if key in NODE_FAILURE_CACHE:
        failure_info = NODE_FAILURE_CACHE[key]
        failures = failure_info['count']
        last_failure = failure_info['timestamp']

        # Get the node name for debugging
        node_name = failure_info.get('node_name', 'Unknown')

        # Exponential backoff: wait longer between retries as failures increase
        backoff_time = min(60 * 2 ** (failures - 1), 3600)  # Cap at 1 hour

        # If we're still in backoff period, skip this node
        if now - last_failure < timedelta(seconds=backoff_time):
            print(f"Skipping node {node_name} (ID: {node_id}) due to recent failures ({failures})")
            return True
        else:
            # Backoff period expired, reset the failure count
            print(f"Backoff period expired for node {node_name} (ID: {node_id}), allowing retry")
            del NODE_FAILURE_CACHE[key]
            return False

    # No recent failures or backoff period expired, don't skip
    return False

def record_node_failure(node_id, operation_type='default', node_name=None):
    """Records a node failure to implement circuit breaker pattern."""
    now = datetime.now()
    key = f"{node_id}:{operation_type}"

    # Try to get the node name from DB if not provided
    if node_name is None:
        try:
            node = Node.objects.get(id=node_id)
            node_name = node.name
        except Node.DoesNotExist:
            node_name = f"Node-{node_id}"

    if key in NODE_FAILURE_CACHE:
        NODE_FAILURE_CACHE[key]['count'] += 1
        NODE_FAILURE_CACHE[key]['timestamp'] = now
        NODE_FAILURE_CACHE[key]['node_name'] = node_name
    else:
        NODE_FAILURE_CACHE[key] = {
            'count': 1,
            'timestamp': now,
            'node_name': node_name
        }

def record_node_success(node_id, operation_type='default'):
    """Resets failure count when a node operation succeeds."""
    key = f"{node_id}:{operation_type}"
    if key in NODE_FAILURE_CACHE:
        del NODE_FAILURE_CACHE[key]

def distribute_chunk_to_node(chunk, node, retry_count=0):
    """Distributes a chunk to a specific node, with retries and verification."""
    try:
        # Skip if this chunk is already on this node
        if ChunkDistribution.objects.filter(chunk=chunk, node=node).exists():
            return True

        # Skip if node has recent failures (circuit breaker pattern)
        if should_skip_node(node.id, 'pin'):
            return False

        # Pin the chunk on the node
        try:
            # Local node
            if node.ip == "127.0.0.1":
                response = requests.post(f"{IPFS_API_URL}/pin/add?arg={chunk.ipfs_hash}",
                                        timeout=TIMEOUTS['PIN_OPERATION'])
            else:
                # Remote node
                response = requests.post(f"{node.get_api_url()}/pin/add?arg={chunk.ipfs_hash}",
                                        timeout=TIMEOUTS['PIN_OPERATION'])

            if response.status_code == 200:
                # Record the distribution regardless of verification
                distribution, created = ChunkDistribution.objects.get_or_create(chunk=chunk, node=node)

                # Update node load based on actual capacity usage
                if created:
                    node.update_load()  # Use the new method to calculate load
                    node.consecutive_failures = 0  # Reset failure counter on success
                    node.save()

                # Record success in our circuit breaker
                record_node_success(node.id, 'pin')

                return True

        except requests.exceptions.RequestException as e:
            print(f"Request error distributing chunk {chunk.ipfs_hash} to {node.name}: {str(e)}")

            # Retry logic for recoverable errors
            if retry_count < 2:  # Limit retries
                time.sleep(1)  # Wait before retrying
                return distribute_chunk_to_node(chunk, node, retry_count + 1)

        # If we get here, there was an issue but we'll create the distribution record anyway
        try:
            distribution, created = ChunkDistribution.objects.get_or_create(chunk=chunk, node=node)
            if created:
                # Update load only if we created a new record
                node.update_load()  # Use the new method to calculate load
                node.save()
                return True
        except Exception as db_error:
            print(f"Database error creating distribution: {str(db_error)}")

        return False

    except Exception as e:
        print(f"Error distributing chunk {chunk.ipfs_hash} to {node.name}: {str(e)}")

        # Record the failure in our circuit breaker
        record_node_failure(node.id, 'pin', node.name)

        # Mark node as potentially inactive if multiple failures
        node.consecutive_failures += 1
        if node.consecutive_failures > 5:
            node.is_active = False
        node.save()

        return False

def discover_ipfs_nodes():
    """Fetches connected IPFS nodes and adds them to the database."""
    # Implementation of discover_ipfs_nodes
    pass

def debug_node_connection(node_ip, port):
    """Tests connection to a node and returns diagnostic information."""
    # Implementation
    pass

def add_test_nodes(count=3):
    """Adds test nodes to the system."""
    # Implementation
    pass

def remove_specific_nodes(ip_list):
    """Removes specific nodes by IP address."""
    # Implementation
    pass

def is_blacklisted_ip(ip):
    """Checks if an IP is in the blacklist."""
    # Implementation
    pass

def ensure_chunk_replication(chunk, min_replicas=DISTRIBUTION_FACTOR):
    """Ensures a chunk is replicated to the desired number of active nodes."""
    # Get current distributions for this chunk
    current_distributions = ChunkDistribution.objects.filter(chunk=chunk).select_related('node')
    active_distributions = [d for d in current_distributions if d.node.is_active]

    # Calculate how many more replicas we need
    needed_replicas = max(0, min_replicas - len(active_distributions))

    if needed_replicas == 0:
        return True, "No additional replicas needed"

    # Get optimal nodes for distribution
    existing_node_ids = [d.node.id for d in active_distributions]
    optimal_nodes = get_optimal_nodes(count=needed_replicas + 1, prefer_healthy=True)

    # Filter out nodes that already have this chunk
    new_nodes = [node for node in optimal_nodes if node.id not in existing_node_ids][:needed_replicas]

    # Distribute to new nodes
    success_count = 0
    for node in new_nodes:
        if distribute_chunk_to_node(chunk, node):
            success_count += 1

    if success_count > 0:
        return True, f"Added {success_count} new replicas for chunk {chunk.ipfs_hash}"
    elif needed_replicas > 0:
        return False, f"Failed to add {needed_replicas} needed replicas for chunk {chunk.ipfs_hash}"
    else:
        return True, "No additional replicas needed"
