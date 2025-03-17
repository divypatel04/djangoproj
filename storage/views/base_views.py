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
    # Existing download_file implementation...
    pass

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
    # Existing system_health_check implementation...
    pass

@csrf_exempt
def system_status(request):
    """Check system status including IPFS availability."""
    try:
        # First check if IPFS is accessible
        try:
            response = requests.post(f"{IPFS_API_URL}/version", timeout=TIMEOUTS['QUICK_CHECK'])
            ipfs_available = response.status_code == 200
        except Exception:
            ipfs_available = False

        # Get node information
        nodes = Node.objects.all()
        active_nodes = sum(1 for node in nodes if node.is_active)
        available_storage = sum(max(100 - node.load, 0) * 0.1 for node in nodes if node.is_active)

        node_warning = ""
        if not ipfs_available:
            node_warning = "IPFS daemon is not accessible!"
        elif active_nodes == 0:
            node_warning = "No storage nodes configured! Contact administrator."

        return JsonResponse({
            'active_nodes': active_nodes,
            'available_storage': round(available_storage, 2),
            'system_healthy': ipfs_available and active_nodes >= 1,
            'warning': node_warning,
            'ipfs_available': ipfs_available
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
    # Existing delete_file implementation...
    pass

@login_required
def debug_download(request, file_id):
    # Existing debug_download implementation...
    pass
