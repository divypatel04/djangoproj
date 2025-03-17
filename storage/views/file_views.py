"""File operation views for the storage application."""

import hashlib, io
import requests
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.http import HttpResponse, Http404, JsonResponse
from django.db import transaction

from ..models import File, FileChunk, ChunkDistribution, Node
from .base_views import IPFS_API_URL, CHUNK_SIZE, DISTRIBUTION_FACTOR, TIMEOUTS
from .utils import chunk_file, get_optimal_nodes, distribute_chunk_to_node, ensure_chunk_replication

@login_required
@transaction.atomic
def upload_file(request):
    """Upload a file, chunk it, and distribute to IPFS nodes."""
    if request.method == "POST":
        file = request.FILES.get("file")
        if not file:
            return HttpResponse("No file provided", status=400)

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
                    # Failed to get hash
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
                # Handle timeout specifically
                transaction.set_rollback(True)
                messages.error(request, f"Timeout while uploading chunk {chunk_index}. Try again or use a smaller file.")
                return redirect("upload_file")
            except Exception as e:
                # Rollback transaction on error
                transaction.set_rollback(True)
                messages.error(request, f"Error uploading chunk {chunk_index}: {str(e)}")
                return redirect("upload_file")

        # Now that we've processed all chunks, save the file record with checksum
        file_record.checksum = sha256.hexdigest()
        file_record.save()

        # Save all chunks now that we have a valid file_record
        for chunk_info in chunks:
            # Check for duplicate hash - TEMPORARY FIX UNTIL MIGRATION IS APPLIED
            existing_chunk = FileChunk.objects.filter(ipfs_hash=chunk_info['hash']).first()
            if existing_chunk:
                # Skip creating this chunk and just create a new distribution
                # that points to the existing chunk instead
                for node in get_optimal_nodes(DISTRIBUTION_FACTOR, prefer_healthy=True):
                    # Only create distribution if it doesn't exist
                    if not ChunkDistribution.objects.filter(chunk=existing_chunk, node=node).exists():
                        ChunkDistribution.objects.create(chunk=existing_chunk, node=node)
                        distribution_count += 1
                # Create reference to the chunk in the current file
                FileChunk.objects.create(
                    file=file_record,
                    chunk_index=chunk_info['index'],
                    ipfs_hash=f"{chunk_info['hash']}_{file_record.id}_{chunk_info['index']}",  # Create unique hash reference
                    size=chunk_info['size']
                    # Cannot use reference_hash until migration is applied
                )
                continue

            # Create a new chunk normally
            chunk = FileChunk.objects.create(
                file=file_record,
                chunk_index=chunk_info['index'],
                ipfs_hash=chunk_info['hash'],
                size=chunk_info['size']
            )

            if not local_only:
                # Get optimal nodes for distribution
                optimal_nodes = get_optimal_nodes(DISTRIBUTION_FACTOR, prefer_healthy=True)
            else:
                # Use only local node if specified
                optimal_nodes = [Node.objects.filter(ip="127.0.0.1").first()]
                if not optimal_nodes[0]:
                    # Create local node if it doesn't exist
                    optimal_nodes = [Node.objects.create(
                        name="LocalNode",
                        ipfs_id="local",
                        ip="127.0.0.1",
                        load=0.0
                    )]

            # Distribute chunk to the selected nodes
            distribution_success = False
            for node in optimal_nodes:
                if distribute_chunk_to_node(chunk, node):
                    distribution_count += 1
                    distribution_success = True
                else:
                    distribution_errors.append(f"Failed to distribute chunk {chunk.chunk_index} to {node.name}")

            # If distribution completely failed for a chunk, make sure we at least have it locally
            if not distribution_success and not local_only:
                local_node = Node.objects.filter(ip="127.0.0.1").first()
                if not local_node:
                    local_node = Node.objects.create(
                        name="LocalNode",
                        ipfs_id="local",
                        ip="127.0.0.1",
                        load=0.0
                    )
                if distribute_chunk_to_node(chunk, local_node):
                    distribution_count += 1
                    messages.warning(request, f"Used local storage for chunk {chunk.chunk_index} due to node connection issues.")

        # Add success message with chunk info
        avg_distribution = distribution_count / chunk_count if chunk_count else 0

        if distribution_errors and len(distribution_errors) < 5:
            # Show specific errors if there aren't too many
            for error in distribution_errors:
                messages.warning(request, error)
        elif distribution_errors:
            # Show a summary if there are many errors
            messages.warning(request,
                           f"{len(distribution_errors)} distribution errors occurred. Some chunks may have reduced redundancy.")

        messages.success(request,
                        f"File uploaded successfully and split into {chunk_count} chunks, "
                        f"distributed across an average of {avg_distribution:.1f} nodes per chunk.")
        return redirect("file_details", file_id=file_record.id)

    return render(request, "storage/upload.html")

@login_required
def download_file(request, ipfs_hash):
    """Download file from IPFS with chunk reassembly, using optimal node selection."""
    try:
        # Get the file record
        file_obj = File.objects.filter(ipfs_hash=ipfs_hash).first()

        if not file_obj:
            raise Http404("File not found")

        # Check if this file has chunks
        chunks = FileChunk.objects.filter(file=file_obj).order_by('chunk_index')

        if not chunks.exists():
            # If no chunks, just download the file directly
            response = requests.get(f"{IPFS_API_URL}/cat?arg={ipfs_hash}")
            file_data = response.content
        else:
            # If chunks exist, reassemble the file, fetching each chunk from the best node
            assembled_data = bytearray()

            for chunk in chunks:
                # Find nodes where this chunk is distributed
                distributions = ChunkDistribution.objects.filter(chunk=chunk).select_related('node')

                # Try to get the chunk from each node, starting with least loaded
                chunk_data = None

                if distributions.exists():
                    # Try each node in order of load
                    for dist in sorted(distributions, key=lambda d: d.node.load):
                        try:
                            # Fetch from this node
                            node_url = dist.node.get_api_url()
                            response = requests.get(f"{node_url}/cat?arg={chunk.ipfs_hash}", timeout=5)
                            if response.status_code == 200:
                                chunk_data = response.content
                                break
                        except Exception as e:
                            print(f"Failed to get chunk from node {dist.node.name}: {str(e)}")
                            continue

                # If all distributed nodes failed, fall back to the default IPFS API
                if chunk_data is None:
                    response = requests.get(f"{IPFS_API_URL}/cat?arg={chunk.ipfs_hash}")
                    chunk_data = response.content

                # Add the chunk data to our assembled file
                assembled_data.extend(chunk_data)

            # Verify the checksum
            if hashlib.sha256(assembled_data).hexdigest() != file_obj.checksum:
                messages.error(request, "Downloaded file failed checksum verification!")

            # Set the file data to our assembled content
            file_data = bytes(assembled_data)

        # Return as a downloadable response
        response = HttpResponse(file_data, content_type="application/octet-stream")
        response["Content-Disposition"] = f'attachment; filename="{file_obj.fname}"'
        return response

    except Exception as e:
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
def file_list(request):
    """Displays all files uploaded by the logged-in user."""
    files = File.objects.filter(user=request.user)
    return render(request, "storage/file_list.html", {"files": files})

@login_required
def redistribute_file(request, file_id):
    """Triggers redistribution of a file's chunks across nodes."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)

    file_obj = get_object_or_404(File, id=file_id, user=request.user)
    chunks = FileChunk.objects.filter(file=file_obj)

    distribution_results = []
    for chunk in chunks:
        # Use the enhanced replication function instead of basic distribution
        success, message = ensure_chunk_replication(chunk, DISTRIBUTION_FACTOR)
        distribution_results.append({
            'chunk_index': chunk.chunk_index,
            'success': success,
            'message': message
        })

    # Calculate summary statistics
    successful_ops = sum(1 for result in distribution_results if result['success'])

    messages.success(request, f"Redistribution complete. {successful_ops} of {len(distribution_results)} operations successful.")

    return JsonResponse({
        'success': True,
        'file_id': file_id,
        'results': distribution_results
    })

@login_required
def delete_file(request, file_id):
    """Deletes a file and all its associated chunks and distributions."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)

    # Get the file and ensure it belongs to the requesting user
    file_obj = get_object_or_404(File, id=file_id, user=request.user)

    try:
        with transaction.atomic():
            # Get all chunks associated with this file
            chunks = FileChunk.objects.filter(file=file_obj)
            filename = file_obj.fname

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

            # Return success response
            return JsonResponse({
                'success': True,
                'message': f"File '{filename}' and {chunks.count()} chunks successfully deleted.",
                'distributions_removed': distributions_count
            })

    except Exception as e:
        # Return error response
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@login_required
def debug_download(request, file_id):
    """Provides detailed diagnostic information about a file and its chunks."""
    file_obj = get_object_or_404(File, id=file_id, user=request.user)
    chunks = FileChunk.objects.filter(file=file_obj).order_by('chunk_index')

    # Gather chunk information
    chunk_details = []
    for chunk in chunks:
        distributions = ChunkDistribution.objects.filter(chunk=chunk).select_related('node')

        # Test availability on each node
        node_status = []
        for dist in distributions:
            node = dist.node
            try:
                # Quick check if the file exists on the node
                if node.is_active:
                    verify_url = f"{node.get_api_url()}/pin/ls?arg={chunk.ipfs_hash}"
                    response = requests.post(verify_url, timeout=TIMEOUTS['QUICK_CHECK'])
                    available = response.status_code == 200 and chunk.ipfs_hash in response.text
                else:
                    available = False
            except Exception:
                available = False

            node_status.append({
                'node': node,
                'available': available
            })

        chunk_details.append({
            'chunk': chunk,
            'distributions': distributions,
            'node_status': node_status,
            'available_count': sum(1 for status in node_status if status['available'])
        })

    # Check if full file is directly accessible via IPFS
    file_directly_accessible = False
    try:
        response = requests.head(f"{IPFS_API_URL}/cat?arg={file_obj.ipfs_hash}", timeout=TIMEOUTS['QUICK_CHECK'])
        file_directly_accessible = response.status_code == 200
    except Exception:
        pass

    # Try to determine content type
    content_type = "application/octet-stream"

    return render(request, "storage/file_debug.html", {
        'file': file_obj,
        'chunk_details': chunk_details,
        'total_chunks': chunks.count(),
        'missing_chunks': sum(1 for detail in chunk_details if detail['available_count'] == 0),
        'file_directly_accessible': file_directly_accessible,
        'content_type': content_type
    })
