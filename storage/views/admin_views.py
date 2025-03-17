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
        chunk_count = node.chunk_distributions.count()
        node_stats.append({
            'node': node,
            'chunk_count': chunk_count,
            'space_used': chunk_count * CHUNK_SIZE / (1024 * 1024)  # MB
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

    return render(request, "storage/admin_dashboard.html", {
        "nodes": nodes,
        "node_stats": node_stats,
        "stats": {
            "total_files": total_files,
            "total_chunks": total_chunks,
            "total_distributions": total_distributions,
            "avg_distribution": round(avg_distribution, 1),
            "under_distributed": under_distributed,
            "distribution_health": "Good" if avg_distribution >= 2 and under_distributed == 0 else "Warning"
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

        # Find overloaded nodes (load > 70%)
        overloaded_nodes = active_nodes.filter(load__gt=70)

        # Find underutilized nodes (load < 30%)
        underutilized_nodes = active_nodes.filter(load__lt=30)

        redistributions = 0
        errors = 0

        # Only proceed if we have both overloaded and underutilized nodes
        if underutilized_nodes.exists() and overloaded_nodes.exists():
            for overloaded_node in overloaded_nodes:
                # Get chunks on this overloaded node
                distributions = ChunkDistribution.objects.filter(
                    node=overloaded_node
                ).select_related('chunk')[:50]  # Limit to 50 to avoid overload

                for dist in distributions:
                    # Check if this chunk is already on any underutilized node
                    for target_node in underutilized_nodes:
                        if not ChunkDistribution.objects.filter(
                            chunk=dist.chunk,
                            node=target_node
                        ).exists():
                            # Distribute to this underutilized node
                            if distribute_chunk_to_node(dist.chunk, target_node):
                                redistributions += 1

                                # Adjust loads
                                target_node.load = min(target_node.load + 0.5, 100.0)
                                target_node.save()

                                # Reduce load on overloaded node
                                overloaded_node.load = max(overloaded_node.load - 0.5, 0.0)
                                overloaded_node.save()

                                # If we've moved enough chunks, stop for this node
                                if redistributions % 10 == 0:
                                    break
                            else:
                                errors += 1

        return JsonResponse({
            'success': True,
            'redistributions': redistributions,
            'errors': errors,
            'overloaded_nodes': overloaded_nodes.count(),
            'underutilized_nodes': underutilized_nodes.count()
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
            if node.consecutive_failures > 5:
                node.is_active = False
            node.save()

            return JsonResponse({
                'success': False,
                'node_id': node.id,
                'message': f'Node returned status code {response.status_code}',
                'status_changed': node.is_active == False and node.consecutive_failures == 6,
            })

    except Exception as e:
        # Node is unreachable
        node.consecutive_failures += 1
        if node.consecutive_failures > 5:
            node.is_active = False
        node.save()

        return JsonResponse({
            'success': False,
            'node_id': node.id,
            'message': f'Failed to connect to node: {str(e)}',
            'status_changed': node.is_active == False and node.consecutive_failures == 6,
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
    active_nodes = Node.objects.filter(is_active=True).order_by('name')
    diagnostic_result = None

    if request.method == 'POST':
        action = request.POST.get('action')

        if action == 'test_connection':
            node_ip = request.POST.get('node_ip')
            port = int(request.POST.get('port', 5001))

            if node_ip in BLACKLISTED_IPS:
                diagnostic_result = {"error": "The IP is blacklisted and will not be used in the system."}
                messages.warning(request, "This IP address is blacklisted.")
                return render(request, "storage/node_diagnostic.html", {
                    "active_nodes": active_nodes,
                    "diagnostic_result": diagnostic_result
                })

            diagnostic_result = debug_node_connection(node_ip, port)

            # If test is successful, try to create or update the node
            if diagnostic_result.get('api_test'):
                node_id = diagnostic_result.get('api_response', {}).get('id')
                if node_id:
                    node, created = Node.objects.update_or_create(
                        ipfs_id=node_id,
                        defaults={
                            'name': f"Node-{node_ip}",
                            'ip': node_ip,
                            'port': port,
                            'is_active': True,
                            'consecutive_failures': 0
                        }
                    )
                    if created:
                        messages.success(request, f"Successfully added new node {node.name}")
                    else:
                        messages.success(request, f"Successfully updated node {node.name}")

                # Refresh active nodes list
                active_nodes = Node.objects.filter(is_active=True).order_by('name')

        elif action == 'add_test_nodes':
            # Add test nodes to ensure we have enough active nodes
            nodes = add_test_nodes(count=3)
            messages.success(request, f"Added/activated {len(nodes)} test nodes")

            # Refresh active nodes list
            active_nodes = Node.objects.filter(is_active=True).order_by('name')

    return render(request, "storage/node_diagnostic.html", {
        "active_nodes": active_nodes,
        "diagnostic_result": diagnostic_result
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

@login_required
@user_passes_test(is_admin)
def node_diagnostic(request):
    """View for diagnosing node connection issues and adding test nodes."""
    nodes = Node.objects.all().order_by('name')

    if request.method == "POST":
        node_url = request.POST.get("node_url", "").strip()
        if not node_url:
            messages.error(request, "Node URL is required")
            return render(request, 'storage/node_diagnostic.html', {'nodes': nodes})

        try:
            # Generate a unique name based on URL and timestamp
            timestamp = int(time.time())
            name_base = f"Node-{hashlib.md5(node_url.encode()).hexdigest()[:8]}"
            name = f"{name_base}-{timestamp}"

            # Extract IP and port from URL (assuming format like http://127.0.0.1:5001)
            url_parts = node_url.split('//')[-1].split(':')
            ip = url_parts[0]
            port = int(url_parts[1]) if len(url_parts) > 1 else 5001

            # Check if a node with this IP and port already exists
            existing_node = Node.objects.filter(ip=ip, port=port).first()
            if existing_node:
                messages.warning(request, f'Node with this address already exists (Name: {existing_node.name})')
                return render(request, 'storage/node_diagnostic.html', {'nodes': nodes})

            try:
                # Create the node with a unique name
                node = Node.objects.create(
                    name=name,
                    ipfs_id=f"ipfs-{timestamp}",
                    ip=ip,
                    port=port,
                    load=0.0,
                    is_active=True
                )

                # Test connection to the node
                try:
                    response = requests.post(f"{node.get_api_url()}/id", timeout=5)
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

    return render(request, 'storage/node_diagnostic.html', {'nodes': nodes})
