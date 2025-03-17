"""Admin views for the storage application."""

import time
import requests
from datetime import datetime, timedelta
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse
from django.db.models import Count
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from django.db import IntegrityError
import hashlib

from ..models import Node, File, FileChunk, ChunkDistribution
from ..constants import IPFS_API_URL, CHUNK_SIZE, DISTRIBUTION_FACTOR, TIMEOUTS, BLACKLISTED_IPS
from .base_views import is_admin
from .utils import (
    distribute_chunk_to_node, should_skip_node, record_node_failure,
    record_node_success, NODE_FAILURE_CACHE, discover_ipfs_nodes,
    ensure_chunk_replication, debug_node_connection, add_test_nodes,
    remove_specific_nodes, is_blacklisted_ip
)

@login_required
@user_passes_test(is_admin, login_url='/files/')
def admin_dashboard(request):
    """Admin dashboard showing storage node statistics."""
    # If user isn't admin, redirect to files page
    if not is_admin(request.user):
        return redirect('file_list')

    # Comment out ensure_required_nodes to prevent automatic node creation
    # ensure_required_nodes()

    # Don't auto-discover nodes
    # discover_ipfs_nodes()

    nodes = Node.objects.all()

    # Get node statistics
    node_stats = []
    for node in nodes:
        used_space_mb = node.get_used_space_mb()
        node_stats.append({
            'node': node,
            'chunk_count': node.chunk_distributions.count(),
            'space_used': used_space_mb,
            'capacity_gb': node.capacity_gb,
            'space_used_percent': min(100, (used_space_mb / (node.capacity_gb * 1024)) * 100)
        })

    # Get distribution statistics
    total_files = File.objects.count()
    total_chunks = FileChunk.objects.count()
    total_distributions = ChunkDistribution.objects.count()
    avg_distribution = total_distributions / total_chunks if total_chunks else 0

    # Find under-distributed chunks (less than 2 nodes)
    under_distributed = FileChunk.objects.annotate(
        dist_count=Count('distributions')
    ).filter(dist_count__lt=2).count()

    # Determine distribution health
    # If there are no chunks or all chunks are properly distributed, it's healthy
    distribution_health = "Good"
    if total_chunks > 0 and under_distributed > 0:
        distribution_health = "Warning"

    return render(request, "storage/admin_dashboard.html", {
        "nodes": nodes,
        "node_stats": node_stats,
        "stats": {
            "total_files": total_files,
            "total_chunks": total_chunks,
            "total_distributions": total_distributions,
            "avg_distribution": round(avg_distribution, 1),
            "under_distributed": under_distributed,
            "distribution_health": distribution_health
        }
    })

# Comment out this function to disable automatic node creation
# def ensure_required_nodes():
#     """Ensures that basic required nodes are registered in the system."""
#     # Ensure the primary local node exists
#     primary_node, created = Node.objects.update_or_create(
#         ip="127.0.0.1", port=5001,
#         defaults={
#             'name': "LocalNode",
#             'ipfs_id': "local_primary",
#             'is_active': True,
#             'load': 0.0,
#             'consecutive_failures': 0
#         }
#     )
#
#     if created:
#         print(f"Created primary local node: {primary_node.name}")
#
#     # Ensure the secondary local node at port 5002 exists
#     secondary_node, created = Node.objects.update_or_create(
#         ip="127.0.0.1", port=5002,
#         defaults={
#             'name': "LocalNode2",
#             'ipfs_id': "local_secondary",
#             'is_active': True,
#             'load': 0.0,
#             'consecutive_failures': 0
#         }
#     )
#
#     if created:
#         print(f"Created secondary local node: {secondary_node.name}")
#
#     return [primary_node, secondary_node]

@user_passes_test(lambda u: u.is_superuser)
def system_health_check(request):
    """Run a system-wide health check of file replication and node status."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)

    # Get files with insufficient chunk replication
    under_replicated_files = []

    # Find chunks with fewer than DISTRIBUTION_FACTOR distributions
    under_distributed_chunks = FileChunk.objects.annotate(
        distribution_count=Count('distributions')
    ).filter(distribution_count__lt=DISTRIBUTION_FACTOR)

    # Group by file
    file_ids_with_issues = under_distributed_chunks.values_list('file_id', flat=True).distinct()
    affected_files = File.objects.filter(id__in=file_ids_with_issues)

    replication_fixed = 0

    # Process each file
    for file in affected_files:
        file_chunks = FileChunk.objects.filter(file=file)
        chunks_fixed = 0
        total_chunks = file_chunks.count()

        # Try to fix each chunk's replication
        for chunk in file_chunks:
            success, _ = ensure_chunk_replication(chunk, DISTRIBUTION_FACTOR)
            if success:
                chunks_fixed += 1
                replication_fixed += 1

        under_replicated_files.append({
            'file_id': file.id,
            'filename': file.fname,
            'chunks_fixed': chunks_fixed,
            'total_chunks': total_chunks
        })

    # Check for inactive nodes and try to reactivate
    inactive_nodes = Node.objects.filter(is_active=False)
    reactivated_nodes = 0

    for node in inactive_nodes:
        # Try to ping the node
        try:
            response = requests.post(f"{node.get_api_url()}/id", timeout=5)
            if response.status_code == 200:
                node.is_active = True
                node.consecutive_failures = 0
                node.save()
                reactivated_nodes += 1
        except Exception:
            # Node is still down
            pass

    return JsonResponse({
        'success': True,
        'under_replicated_files': len(under_replicated_files),
        'chunks_fixed': replication_fixed,
        'reactivated_nodes': reactivated_nodes,
        'details': under_replicated_files
    })

@user_passes_test(is_admin)
def rebalance_distributions(request):
    """Redistributes chunks to balance load across nodes."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)

    try:
        # Get all active nodes
        active_nodes = Node.objects.filter(is_active=True)
        if not active_nodes.exists():
            return JsonResponse({
                'success': False,
                'error': 'No active nodes available for rebalancing',
                'redistributions': 0
            })

        # Find all nodes with load data
        high_load_nodes = active_nodes.filter(load__gt=50).order_by('-load')
        low_load_nodes = active_nodes.filter(load__lt=20).order_by('load')

        # If we don't have both high load and low load nodes, use a different approach
        if not high_load_nodes.exists() or not low_load_nodes.exists():
            # Find the nodes with highest and lowest loads
            sorted_nodes = list(active_nodes.order_by('load'))

            if len(sorted_nodes) < 2:
                return JsonResponse({
                    'success': False,
                    'error': 'Need at least 2 active nodes for rebalancing',
                    'redistributions': 0
                })

            # Take the lowest loaded 50% of nodes and highest loaded 50% of nodes
            midpoint = len(sorted_nodes) // 2
            low_load_nodes = sorted_nodes[:midpoint]
            high_load_nodes = sorted_nodes[midpoint:]

        redistributions = 0
        errors = 0
        log_messages = []

        # Process each high load node
        for source_node in high_load_nodes:
            # Skip if the node is no longer high load (might happen during rebalancing)
            if source_node.load < 40:
                continue

            log_messages.append(f"Processing high load node: {source_node.name} ({source_node.load:.1f}%)")

            # Get chunks distributed on this node, limit to 100 to prevent overwhelming
            distributions = ChunkDistribution.objects.filter(
                node=source_node
            ).select_related('chunk')[:100]

            if not distributions.exists():
                log_messages.append(f"- No distributions found on node {source_node.name}")
                continue

            log_messages.append(f"- Found {distributions.count()} chunks to redistribute")

            # For each chunk on the high load node
            for dist in distributions:
                chunk = dist.chunk

                # Find target nodes that don't already have this chunk
                for target_node in low_load_nodes:
                    # Skip if source and target are the same
                    if target_node.id == source_node.id:
                        continue

                    # Check if this chunk is already on this target node
                    if not ChunkDistribution.objects.filter(
                        chunk=chunk, node=target_node
                    ).exists():
                        # Try to distribute to the low load node
                        log_messages.append(f"- Distributing chunk {chunk.chunk_index} of file {chunk.file.fname} to {target_node.name}")

                        if distribute_chunk_to_node(chunk, target_node):
                            redistributions += 1

                            # Update loads
                            target_node.load = min(target_node.load + 0.5, 100.0)
                            target_node.save()

                            source_node.load = max(source_node.load - 0.5, 0.0)
                            source_node.save()

                            # Don't add too many chunks to a single node at once
                            if redistributions % 5 == 0:
                                break
                        else:
                            errors += 1
                            log_messages.append(f"  - Failed to distribute to {target_node.name}")

                # If we've done enough redistributions, stop for this source node
                if redistributions >= 20:  # Limit total redistributions per run
                    break

        return JsonResponse({
            'success': True,
            'redistributions': redistributions,
            'errors': errors,
            'high_load_nodes': high_load_nodes.count(),
            'low_load_nodes': low_load_nodes.count(),
            'log_messages': log_messages
        })

    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e),
            'redistributions': 0
        }, status=500)

@user_passes_test(is_admin)
def test_node(request, node_id):
    """Tests if a specific node is online and updates its status."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)

    node = get_object_or_404(Node, id=node_id)

    try:
        # Test if we can reach the node
        response = requests.post(f"{node.get_api_url()}/id", timeout=5)

        if response.status_code == 200:
            # Node is responsive
            prev_status = node.is_active
            node.is_active = True
            node.consecutive_failures = 0
            node.save()

            return JsonResponse({
                'success': True,
                'node_id': node.id,
                'message': 'Node is online and responding.',
                'status_changed': not prev_status,
            })
        else:
            # Node is reachable but returned an error
            node.consecutive_failures += 1
            # Mark node as inactive immediately on error
            node.is_active = False
            node.save()

            return JsonResponse({
                'success': False,
                'node_id': node.id,
                'message': f'Node returned status code {response.status_code}. Node marked as inactive.',
                'status_changed': True,
            })

    except Exception as e:
        # Node is unreachable
        node.consecutive_failures += 1
        # Mark node as inactive immediately on error
        node.is_active = False
        node.save()

        return JsonResponse({
            'success': False,
            'node_id': node.id,
            'message': f'Failed to connect to node: {str(e)}. Node marked as inactive.',
            'status_changed': True,
        })

@user_passes_test(is_admin)
def circuit_breaker_status(request):
    """Returns the current status of the circuit breaker."""
    status_data = []

    # Get all keys from the NODE_FAILURE_CACHE
    for key, data in NODE_FAILURE_CACHE.items():
        # Parse the key to get node_id and operation_type
        parts = key.split(':')
        if len(parts) == 2:
            node_id, operation_type = parts

            # Calculate backoff time
            failures = data['count']
            backoff_time = min(60 * 2 ** (failures - 1), 3600)  # Cap at 1 hour

            # Calculate expiry time
            expiry_time = data['timestamp'] + timedelta(seconds=backoff_time)
            now = datetime.now()
            seconds_left = (expiry_time - now).total_seconds()

            # Add to status data
            status_data.append({
                'node_id': node_id,
                'node_name': data.get('node_name', 'Unknown'),
                'operation_type': operation_type,
                'failures': failures,
                'last_failure': data['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                'backoff_seconds': backoff_time,
                'expires_at': expiry_time.strftime('%Y-%m-%d %H:%M:%S'),
                'seconds_left': max(0, int(seconds_left))
            })

    # Return the data as JSON
    return JsonResponse({
        'circuit_breaker_entries': status_data,
        'total_entries': len(status_data)
    })

@user_passes_test(is_admin)
def reset_circuit_breaker(request, node_id=None):
    """Resets circuit breaker for a specific node or all nodes."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)

    # Reset for all nodes if no node_id is provided
    if node_id is None:
        # Clear the entire cache
        NODE_FAILURE_CACHE.clear()
        return JsonResponse({
            'success': True,
            'message': 'Circuit breaker reset for all nodes',
            'entries_cleared': 'all'
        })

    # Reset just for the specified node
    keys_to_delete = []
    for key in NODE_FAILURE_CACHE:
        if key.startswith(f"{node_id}:"):
            keys_to_delete.append(key)

    # Delete the keys
    for key in keys_to_delete:
        del NODE_FAILURE_CACHE[key]

    # Return success response
    return JsonResponse({
        'success': True,
        'message': f'Circuit breaker reset for node ID {node_id}',
        'entries_cleared': len(keys_to_delete)
    })

@login_required
@user_passes_test(is_admin, login_url='/files/')
def node_diagnostic(request):
    """View for diagnosing node connection issues and adding test nodes."""
    nodes = Node.objects.all().order_by('name')

    if request.method == "POST":
        # Check if it's an action form or node addition form
        if request.POST.get('action'):
            # Handle action-based form submissions
            action = request.POST.get('action')
            if action == 'test_connection':
                node_ip = request.POST.get('node_ip')
                port = int(request.POST.get('port', 5001))
                # ...existing action code...
                diagnostic_result = debug_node_connection(node_ip, port)

                # If test is successful, try to update the node status
                if diagnostic_result and not diagnostic_result.get('api_test', False):
                    # Test failed, try to find the node and update its status
                    try:
                        node = Node.objects.get(ip=node_ip, port=port)
                        node.is_active = False
                        node.consecutive_failures += 1
                        node.save()
                        messages.warning(request, f"Node {node.name} is not accessible. Status updated to inactive.")
                    except Node.DoesNotExist:
                        # Node doesn't exist yet, will be created with proper status
                        pass

                # Add the rest of the test_connection code here

            elif action == 'add_test_nodes':
                # Add implementation for add_test_nodes action
                nodes = add_test_nodes(count=3)
                messages.success(request, f"Added/activated {len(nodes)} test nodes")
                # ...rest of action code...
        else:
            # Handle the node addition form
            node_url = request.POST.get("node_url", "").strip()
            if not node_url:
                messages.error(request, "Node URL is required")
                return render(request, 'storage/node_diagnostic.html', {'nodes': nodes})

            capacity_gb = float(request.POST.get("capacity_gb", 10.0))  # Get the capacity from the form

            try:
                # Extract IP and port from URL (assuming format like http://127.0.0.1:5001)
                # Handle URLs with or without /api/v0 suffix
                url_parts = node_url.split('//')[-1].split('/')
                ip_port = url_parts[0]
                ip_port_parts = ip_port.split(':')

                ip = ip_port_parts[0]
                port = int(ip_port_parts[1]) if len(ip_port_parts) > 1 else 5001

                # Generate a unique name based on URL and timestamp
                timestamp = int(time.time())
                name_base = f"Node-{hashlib.md5(ip.encode()).hexdigest()[:8]}"
                name = f"{name_base}-{timestamp}"

                # Format the API URL properly
                api_url = f"http://{ip}:{port}/api/v0"

                # Check if a node with this IP and port already exists
                existing_node = Node.objects.filter(ip=ip, port=port).first()
                if existing_node:
                    messages.warning(request, f'Node with this address already exists (Name: {existing_node.name})')
                    return render(request, 'storage/node_diagnostic.html', {'nodes': nodes})

                try:
                    # Create the node with a unique name and properly formatted API URL
                    node = Node.objects.create(
                        name=name,
                        ipfs_id=f"ipfs-{timestamp}",
                        ip=ip,
                        port=port,
                        api_url=api_url,  # Store the formatted API URL
                        load=0.0,
                        capacity_gb=capacity_gb,  # Set the capacity
                        is_active=True
                    )

                    # Test connection to the node
                    try:
                        response = requests.post(f"{api_url}/id", timeout=5)
                        if response.status_code == 200:
                            messages.success(request, f'Successfully added and connected to node {node.name}')
                        else:
                            messages.warning(request, f'Node added but returned status code {response.status_code}')
                    except Exception as e:
                        messages.warning(request, f'Node added but could not connect: {str(e)}')

                    # Refresh the nodes list
                    nodes = Node.objects.all().order_by('name')

                except IntegrityError as e:
                    messages.error(request, f'Database error: {str(e)}')

            except Exception as e:
                messages.error(request, f'Unexpected error: {str(e)}')

    # Prepare the diagnostic result for action-based submissions
    diagnostic_result = None
    active_nodes = Node.objects.filter(is_active=True).order_by('name')

    return render(request, 'storage/node_diagnostic.html', {
        'nodes': nodes,
        'active_nodes': active_nodes,
        'diagnostic_result': diagnostic_result
    })

@login_required
@user_passes_test(is_admin, login_url='/files/')
def remove_nodes(request):
    """View for removing specific problematic nodes."""
    removed_info = None

    if request.method == 'POST':
        action = request.POST.get('action')

        if action == 'remove_specific_nodes':
            # Define the list of IPs to remove
            ips_to_remove = ['1.36.226.78', '104.168.82.126']
            removed_info = remove_specific_nodes(ips_to_remove)

            # Success message
            if removed_info:
                messages.success(
                    request,
                    f"Successfully removed {len(removed_info)} problematic nodes."
                )
            else:
                messages.info(request, "No matching nodes were found to remove.")

        elif action == 'delete_selected_nodes':
            # Get the selected node IDs
            selected_node_ids = request.POST.getlist('selected_nodes')
            if not selected_node_ids:
                messages.warning(request, "No nodes were selected for deletion.")
            else:
                removed_nodes = []
                # Process each selected node
                for node_id in selected_node_ids:
                    try:
                        node = Node.objects.get(id=node_id)

                        # Get distribution count before deletion
                        distribution_count = ChunkDistribution.objects.filter(node=node).count()

                        # Store info before deletion
                        removed_nodes.append({
                            'name': node.name,
                            'ip': node.ip,
                            'port': node.port,
                            'distributions': distribution_count
                        })

                        # Delete the node
                        node.delete()
                    except Node.DoesNotExist:
                        messages.error(request, f"Node with ID {node_id} not found.")

                # Set removed_info to display the table of removed nodes
                removed_info = removed_nodes

                # Show success message
                if removed_nodes:
                    messages.success(
                        request,
                        f"Successfully deleted {len(removed_nodes)} selected nodes."
                    )
                else:
                    messages.warning(request, "No nodes were deleted.")

        elif action == 'delete_all_nodes':
            # Get all nodes
            all_nodes = Node.objects.all()
            count = all_nodes.count()

            if count == 0:
                messages.warning(request, "No nodes exist to delete.")
            else:
                # Delete all nodes
                all_nodes.delete()
                messages.success(request, f"Successfully deleted all {count} nodes.")

    # Get the current list of nodes
    nodes = Node.objects.all().order_by('ip')

    return render(request, "storage/manage_nodes.html", {
        "nodes": nodes,
        "removed_info": removed_info,
        "blacklisted_ips": BLACKLISTED_IPS
    })
