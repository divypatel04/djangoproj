"""Base views for the storage application."""

import requests
import hashlib, json, io, random, time
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth import login, logout, authenticate
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from django.http import HttpResponse, Http404, JsonResponse
from ..models import File, Node, FileChunk, ChunkDistribution
import os
from django.db import transaction
from django.db.models import Count, Q
from functools import lru_cache
from datetime import datetime, timedelta
from django.views.decorators.csrf import csrf_exempt

# Import constants
from ..constants import IPFS_API_URL, CHUNK_SIZE, DISTRIBUTION_FACTOR, TIMEOUTS

# Import utilities
from .utils import (
    distribute_chunk_to_node, should_skip_node, record_node_failure,
    record_node_success, NODE_FAILURE_CACHE, discover_ipfs_nodes, chunk_file,
    get_optimal_nodes, ensure_chunk_replication  # Add this import
)

# Maximum retries for operations
MAX_RETRY_ATTEMPTS = 3

def is_admin(user):
    """Check if user is an admin or staff member"""
    return user.is_authenticated and (user.is_superuser or user.is_staff)

# 📌 Homepage
def index(request):
    """Homepage that displays file status and admin access."""
    files = File.objects.filter(user=request.user) if request.user.is_authenticated else None
    return render(request, "storage/index.html", {"files": files})

# 📌 Authentication Views
def user_register(request):
    if request.method == "POST":
        username = request.POST["username"]
        password = request.POST["password"]

        if User.objects.filter(username=username).exists():
            messages.error(request, "Username already taken")
        else:
            user = User.objects.create_user(username=username, password=password)
            login(request, user)
            return redirect("file_list")

    return render(request, "storage/register.html")

def user_login(request):
    if request.method == "POST":
        username = request.POST["username"]
        password = request.POST["password"]
        user = authenticate(request, username=username, password=password)

        if user:
            login(request, user)
            return redirect("file_list")
        else:
            messages.error(request, "Invalid credentials")

    return render(request, "storage/login.html")

def user_logout(request):
    logout(request)
    return redirect("index")

@login_required
def file_list(request):
    """Displays all files uploaded by the logged-in user."""
    files = File.objects.filter(user=request.user)
    return render(request, "storage/file_list.html", {"files": files})

# Rest of your views...
# For brevity, I've only included a subset of the views here.
# The full implementation would include all the views from the original views.py

@login_required
@transaction.atomic
def upload_file(request):
    """Handle file upload with chunking and distribution."""
    if request.method == "POST":
        file = request.FILES.get("file")
        if not file:
            messages.error(request, "No file provided")
            return redirect("upload_file")

        # Check IPFS availability first
        try:
            response = requests.post(f"{IPFS_API_URL}/version", timeout=TIMEOUTS['QUICK_CHECK'])
            if response.status_code != 200:
                messages.error(request, "IPFS daemon is not accessible. Please ensure IPFS is running.")
                return redirect("upload_file")
        except Exception as e:
            messages.error(request, f"Could not connect to IPFS: {str(e)}")
            return redirect("upload_file")

        # Check if any nodes exist
        nodes_exist = Node.objects.filter(is_active=True).exists()
        if not nodes_exist:
            messages.error(request, "No active storage nodes are available. Please contact the administrator to set up nodes.")
            return redirect("file_list")

        # Check if we should use local storage only (from the client)
        local_only = request.POST.get('local_only') == 'true'

        # Create file record but don't save until we have the checksum
        file_record = File(
            user=request.user,
            fname=file.name,
            size=file.size
        )

        # Calculate checksum while chunking
        sha256 = hashlib.sha256()
        chunk_count = 0
        distribution_count = 0
        chunks = []  # Store chunk info before committing to DB
        distribution_errors = []  # Track distribution errors

        # Stream through the file in chunks
        for chunk_index, chunk_data in chunk_file(file):
            # Update running checksum
            sha256.update(chunk_data)

            # Upload chunk to IPFS
            files = {"file": io.BytesIO(chunk_data)}
            try:
                response = requests.post(f"{IPFS_API_URL}/add",
                                      files=files,
                                      timeout=TIMEOUTS['UPLOAD_OPERATION'])
                ipfs_response = response.json()
                chunk_hash = ipfs_response.get("Hash")

                if not chunk_hash:
                    raise ValueError("Failed to get IPFS hash for chunk")

                # Save the first chunk's hash as the file's main hash
                if chunk_index == 0:
                    file_record.ipfs_hash = chunk_hash

                # Store chunk info temporarily
                chunks.append({
                    'index': chunk_index,
                    'hash': chunk_hash,
                    'size': len(chunk_data)
                })

                chunk_count += 1
            except requests.exceptions.Timeout:
                transaction.set_rollback(True)
                messages.error(request, f"Timeout while uploading chunk {chunk_index}. Try again or use a smaller file.")
                return redirect("upload_file")
            except Exception as e:
                transaction.set_rollback(True)
                messages.error(request, f"Error uploading chunk {chunk_index}: {str(e)}")
                return redirect("upload_file")

        # Save the file record with checksum
        file_record.checksum = sha256.hexdigest()
        file_record.save()

        # Ensure we have at least one local node
        local_node, created = Node.objects.get_or_create(
            ip="127.0.0.1",
            port=5001,
            defaults={
                'name': "LocalNode",
                'ipfs_id': "local_primary",
                'is_active': True,
                'load': 0.0,
                'consecutive_failures': 0
            }
        )

        if created:
            print("Created local node for file storage")

        # Process chunks and distribute them
        for chunk_info in chunks:
            chunk = FileChunk.objects.create(
                file=file_record,
                chunk_index=chunk_info['index'],
                ipfs_hash=chunk_info['hash'],
                size=chunk_info['size']
            )

            # Always include the local node in our distribution list
            if not local_only:
                optimal_nodes = get_optimal_nodes(DISTRIBUTION_FACTOR, prefer_healthy=True)
                # Make sure local node is in the list
                if local_node not in optimal_nodes:
                    optimal_nodes.append(local_node)
            else:
                optimal_nodes = [local_node]

            # Distribute chunk to nodes
            distribution_success = False
            for node in optimal_nodes:
                if distribute_chunk_to_node(chunk, node):
                    distribution_count += 1
                    distribution_success = True
                else:
                    distribution_errors.append(f"Failed to distribute chunk {chunk.chunk_index} to {node.name}")

            if not distribution_success and not local_only:
                local_node = Node.objects.filter(ip="127.0.0.1").first()
                if not local_node:
                    local_node = Node.objects.create(
                        name="LocalNode",
                        ipfs_id="local",
                        ip="127.0.0.1",
                        port=5001,
                        load=0.0
                    )
                if distribute_chunk_to_node(chunk, local_node):
                    distribution_count += 1
                    messages.warning(request, f"Used local storage for chunk {chunk.chunk_index} due to node connection issues.")

        # Show distribution errors if any
        if distribution_errors and len(distribution_errors) < 5:
            for error in distribution_errors:
                messages.warning(request, error)
        elif distribution_errors:
            messages.warning(request, f"{len(distribution_errors)} distribution errors occurred. Some chunks may have reduced redundancy.")

        # Show success message
        avg_distribution = distribution_count / chunk_count if chunk_count else 0
        messages.success(request,
                        f"File uploaded successfully and split into {chunk_count} chunks, "
                        f"distributed across an average of {avg_distribution:.1f} nodes per chunk.")
        return redirect("file_details", file_id=file_record.id)

    # If GET request, show the upload form
    return render(request, "storage/upload.html")

@login_required
def download_file(request, ipfs_hash):
    """Download file from IPFS with chunk reassembly, using optimal node selection."""
    try:
        # Get the file record
        file_obj = get_object_or_404(File, ipfs_hash=ipfs_hash)

        # Verify user has access to this file
        if file_obj.user != request.user and not request.user.is_staff:
            return HttpResponse("Access denied", status=403)

        # Check if this file has chunks
        chunks = FileChunk.objects.filter(file=file_obj).order_by('chunk_index')

        if not chunks.exists():
            # If no chunks, just download the file directly
            try:
                response = requests.post(f"{IPFS_API_URL}/cat?arg={ipfs_hash}", timeout=TIMEOUTS['UPLOAD_OPERATION'])
                if response.status_code != 200:
                    return HttpResponse(f"IPFS error: {response.text}", status=500)
                file_data = response.content
            except requests.exceptions.RequestException as e:
                return HttpResponse(f"Error retrieving file: {str(e)}", status=500)
        else:
            # If chunks exist, reassemble the file, fetching each chunk from the best node
            assembled_data = bytearray()
            failed_chunks = []

            for chunk in chunks:
                # Find nodes where this chunk is distributed
                distributions = ChunkDistribution.objects.filter(chunk=chunk).select_related('node')

                # Try to get the chunk from each node, starting with least loaded
                chunk_data = None
                chunk_error = None

                if distributions.exists():
                    # Try each node in order of load
                    for dist in sorted(distributions, key=lambda d: d.node.load):
                        node = dist.node
                        if not node.is_active:
                            continue

                        try:
                            # Fetch from this node
                            node_url = node.get_api_url()
                            response = requests.post(
                                f"{node_url}/cat?arg={chunk.ipfs_hash}",
                                timeout=TIMEOUTS['UPLOAD_OPERATION']
                            )

                            if response.status_code == 200:
                                chunk_data = response.content
                                break
                            else:
                                chunk_error = f"Node {node.name} returned status {response.status_code}"
                        except Exception as e:
                            chunk_error = f"Node {node.name} error: {str(e)}"
                            continue

                # If all distributed nodes failed, fall back to the default IPFS API
                if chunk_data is None:
                    try:
                        response = requests.post(
                            f"{IPFS_API_URL}/cat?arg={chunk.ipfs_hash}",
                            timeout=TIMEOUTS['UPLOAD_OPERATION']
                        )

                        if response.status_code == 200:
                            chunk_data = response.content
                        else:
                            failed_chunks.append(f"Chunk {chunk.chunk_index}: {response.text}")
                    except Exception as e:
                        failed_chunks.append(f"Chunk {chunk.chunk_index}: {chunk_error or str(e)}")

                # Still no chunk data? That's a problem
                if chunk_data is None:
                    return HttpResponse(
                        f"Failed to retrieve chunk {chunk.chunk_index} from any node. Errors: {', '.join(failed_chunks)}",
                        status=500
                    )

                # Add the chunk data to our assembled file
                assembled_data.extend(chunk_data)

            # Verify the checksum if available
            if file_obj.checksum:
                calculated_hash = hashlib.sha256(assembled_data).hexdigest()
                if calculated_hash != file_obj.checksum:
                    return HttpResponse(
                        f"Downloaded file failed checksum verification! Expected {file_obj.checksum}, got {calculated_hash}",
                        status=500
                    )

            # Set the file data to our assembled content
            file_data = bytes(assembled_data)

        # Determine content type based on file extension
        content_type = "application/octet-stream"  # Default
        filename = file_obj.fname

        # Get extension from filename
        if '.' in filename:
            ext = filename.split('.')[-1].lower()
            if ext in ['jpg', 'jpeg']:
                content_type = 'image/jpeg'
            elif ext == 'png':
                content_type = 'image/png'
            elif ext == 'pdf':
                content_type = 'application/pdf'
            elif ext in ['doc', 'docx']:
                content_type = 'application/msword'
            elif ext in ['xls', 'xlsx']:
                content_type = 'application/vnd.ms-excel'
            elif ext == 'txt':
                content_type = 'text/plain'
            elif ext == 'mp3':
                content_type = 'audio/mpeg'
            elif ext == 'mp4':
                content_type = 'video/mp4'
            elif ext == 'zip':
                content_type = 'application/zip'

        # Return as a downloadable response
        response = HttpResponse(file_data, content_type=content_type)
        response["Content-Disposition"] = f'attachment; filename="{filename}"'
        response["Content-Length"] = len(file_data)

        # Add cache headers to prevent caching of potentially sensitive files
        response["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response["Pragma"] = "no-cache"
        response["Expires"] = "0"

        return response

    except File.DoesNotExist:
        return HttpResponse("File not found", status=404)
    except Exception as e:
        print(f"Error downloading file: {str(e)}")
        return HttpResponse(f"File retrieval failed: {str(e)}", status=500)

@login_required
def file_details(request, file_id):
    """Shows details about a file including its chunks and distribution."""
    file_obj = get_object_or_404(File, id=file_id, user=request.user)
    chunks = FileChunk.objects.filter(file=file_obj).order_by('chunk_index')

    # Get distribution information for each chunk
    chunks_with_distribution = []
    for chunk in chunks:
        distributions = ChunkDistribution.objects.filter(chunk=chunk).select_related('node')
        chunks_with_distribution.append({
            'chunk': chunk,
            'distributions': distributions,
            'node_count': distributions.count()
        })

    # Calculate distribution metrics
    distribution_stats = file_obj.get_distribution_status()

    return render(request, "storage/file_details.html", {
        'file': file_obj,
        'chunks': chunks_with_distribution,
        'total_chunks': chunks.count(),
        'distribution_stats': distribution_stats
    })

@login_required
def redistribute_file(request, file_id):
    """Triggers redistribution of a file's chunks across nodes."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)

    file_obj = get_object_or_404(File, id=file_id, user=request.user)
    chunks = FileChunk.objects.filter(file=file_obj)

    # Ensure we have at least one local node
    local_node, created = Node.objects.get_or_create(
        ip="127.0.0.1",
        port=5001,
        defaults={
            'name': "LocalNode",
            'ipfs_id': "local_primary",
            'is_active': True,
            'load': 0.0,
            'consecutive_failures': 0
        }
    )

    distribution_results = []
    success_count = 0
    error_count = 0

    try:
        for chunk in chunks:
            # Get current distributions
            current_distributions = ChunkDistribution.objects.filter(chunk=chunk).count()

            # Try to ensure proper replication
            success, message = ensure_chunk_replication(chunk, DISTRIBUTION_FACTOR)

            # Record the result
            distribution_results.append({
                'chunk_index': chunk.chunk_index,
                'success': success,
                'message': message,
                'before': current_distributions,
                'after': ChunkDistribution.objects.filter(chunk=chunk).count()
            })

            if success:
                success_count += 1
            else:
                error_count += 1

        # Add success message
        messages.success(request, f"Redistribution complete. {success_count} of {chunks.count()} chunks successfully distributed.")

        # Return detailed results as JSON
        return JsonResponse({
            'success': True,
            'file_id': file_id,
            'filename': file_obj.fname,
            'total_chunks': chunks.count(),
            'success_count': success_count,
            'error_count': error_count,
            'results': distribution_results
        })

    except Exception as e:
        messages.error(request, f"Error during redistribution: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@user_passes_test(lambda u: u.is_superuser)
def system_health_check(request):
    """Run a system-wide health check of file replication and node status."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)

    try:
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
                response = requests.post(f"{node.get_api_url()}/id", timeout=TIMEOUTS['QUICK_CHECK'])
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
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@csrf_exempt
def system_status(request):
    """Check system status based only on database nodes, not IPFS daemon."""
    try:
        # Get all active nodes from the database
        active_nodes = Node.objects.filter(is_active=True)
        node_count = active_nodes.count()

        # Check if any nodes are responsive
        system_operational = False
        responsive_nodes = 0

        if node_count > 0:
            # Test a sample of nodes (max 3)
            test_nodes = active_nodes[:3]

            for node in test_nodes:
                try:
                    # Skip if circuit breaker indicates we should
                    if should_skip_node(node.id, 'check'):
                        continue

                    response = requests.post(f"{node.get_api_url()}/id", timeout=TIMEOUTS['QUICK_CHECK'])
                    if response.status_code == 200:
                        responsive_nodes += 1
                        # Record success
                        record_node_success(node.id, 'check')
                        # If at least one node is responsive, the system is operational
                        system_operational = True
                        break
                except Exception:
                    # Record failure but continue checking other nodes
                    record_node_failure(node.id, 'check')

        # Calculate available storage based on node capacities
        available_storage = sum(node.get_available_space_gb() for node in active_nodes)

        # Determine warning message based on conditions
        node_warning = ""
        if node_count == 0:
            node_warning = "No storage nodes configured! Contact administrator."
        elif not system_operational:
            node_warning = "All storage nodes are unresponsive! Contact administrator."

        return JsonResponse({
            'active_nodes': node_count,
            'responsive_nodes': responsive_nodes,
            'available_storage': round(available_storage, 2),
            'system_healthy': system_operational,
            'warning': node_warning,
            'ipfs_available': system_operational  # Set this to match system_operational for compatibility
        })
    except Exception as e:
        return JsonResponse({
            'active_nodes': 0,
            'available_storage': 0,
            'system_healthy': False,
            'error': str(e),
            'ipfs_available': False
        }, status=200)  # Still return 200 to allow client to handle the error

@csrf_exempt
def quick_node_check(request):
    """Quickly checks if any nodes are responsive before file upload."""
    if request.method != 'POST':
        return JsonResponse({
            'error': 'Only POST method allowed',
            'all_nodes_ok': False,
            'any_nodes_ok': False
        })

    active_nodes = Node.objects.filter(is_active=True)

    # If we have no active nodes, return immediately
    if not active_nodes.exists():
        return JsonResponse({
            'all_nodes_ok': False,
            'any_nodes_ok': False,
            'message': 'No active nodes found'
        })

    # Test a sample of nodes (max 3)
    nodes_to_test = active_nodes[:3]
    responsive_nodes = 0

    for node in nodes_to_test:
        # Skip if circuit breaker indicates we should
        if should_skip_node(node.id, 'check'):
            continue

        try:
            response = requests.post(f"{node.get_api_url()}/id", timeout=TIMEOUTS['QUICK_CHECK'])
            if response.status_code == 200:
                responsive_nodes += 1
                # Record success
                record_node_success(node.id, 'check')
        except Exception:
            # Record failure
            record_node_failure(node.id, 'check')

    return JsonResponse({
        'all_nodes_ok': responsive_nodes == len(nodes_to_test) and responsive_nodes > 0,
        'any_nodes_ok': responsive_nodes > 0,
        'responsive_nodes': responsive_nodes,
        'tested_nodes': len(nodes_to_test)
    })

@login_required
def delete_file(request, file_id):
    """Deletes a file and all its associated chunks and distributions."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)

    # Get the file and ensure it belongs to the requesting user
    file_obj = get_object_or_404(File, id=file_id, user=request.user)
    filename = file_obj.fname  # Store the filename before deletion

    # Track if deletion was successful
    deletion_successful = False

    try:
        with transaction.atomic():
            # Get all chunks associated with this file
            chunks = FileChunk.objects.filter(file=file_obj)

            # For each chunk, get its distributions
            distributions_count = 0
            for chunk in chunks:
                # Count the distributions we're about to delete
                distributions_count += chunk.distributions.count()

                # Try to unpin from IPFS nodes (but don't stop deletion if this fails)
                for dist in chunk.distributions.select_related('node'):
                    try:
                        # Attempt to unpin from the node
                        node = dist.node
                        if node.is_active:
                            node_url = node.get_api_url()
                            requests.post(
                                f"{node_url}/pin/rm?arg={chunk.ipfs_hash}",
                                timeout=TIMEOUTS['QUICK_CHECK']
                            )
                    except Exception as e:
                        # Log error but continue with deletion
                        print(f"Error unpinning chunk {chunk.ipfs_hash} from node {node.name}: {str(e)}")

            # Delete the file (this will cascade delete chunks and their distributions)
            file_obj.delete()
            deletion_successful = True  # Mark deletion as successful

            # Add success message
            messages.success(request, f"File '{filename}' successfully deleted.")

    except Exception as e:
        # Only show error message if deletion wasn't successful
        if not deletion_successful:
            messages.error(request, f"Error deleting file: {str(e)}")

    # Always redirect to file list, regardless of success or failure
    return redirect('file_list')

@login_required
def debug_download(request, file_id):
    # Existing debug_download implementation...
    pass
