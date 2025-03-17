import requests
import hashlib, json, io, random, time
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth import login, logout, authenticate
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from django.http import HttpResponse, Http404, JsonResponse
from .models import File, Node, FileChunk, ChunkDistribution
import os
from django.db import transaction
from django.db.models import Count, Q
from functools import lru_cache
from datetime import datetime, timedelta
from django.views.decorators.csrf import csrf_exempt

IPFS_API_URL = "http://127.0.0.1:5001/api/v0"
CHUNK_SIZE = 1024 * 1024  # 1MB chunks
DISTRIBUTION_FACTOR = 2  # Number of nodes to distribute each chunk to
MAX_RETRY_ATTEMPTS = 3  # Number of times to retry failed operations

# Cache to store node failure information to prevent repeated requests to failing nodes
NODE_FAILURE_CACHE = {}
# Timeout thresholds for different operations (in seconds)
TIMEOUTS = {
    'QUICK_CHECK': 2,      # Fast health check timeout
    'PIN_OPERATION': 10,   # Timeout for IPFS pin operations
    'UPLOAD_OPERATION': 15 # Timeout for file upload operations
}

# 📌 Helper Function: File Chunking - ENHANCED
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


# 📌 Select optimal nodes for chunk distribution
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


# NEW: Circuit breaker implementation to prevent repeated failed requests
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


# NEW: Record a node failure to implement backoff
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


# NEW: Reset failure count when a node succeeds
def record_node_success(node_id, operation_type='default'):
    """Resets failure count when a node operation succeeds."""
    key = f"{node_id}:{operation_type}"
    if key in NODE_FAILURE_CACHE:
        del NODE_FAILURE_CACHE[key]


# 📌 Enhanced: Distribute a chunk to a node with retries and verification
def distribute_chunk_to_node(chunk, node, retry_count=0):
    """Distributes a chunk to a specific node, with retries and verification."""
    try:
        # Skip if this chunk is already on this node
        if ChunkDistribution.objects.filter(chunk=chunk, node=node).exists():
            return True

        # Skip if node has recent failures (circuit breaker pattern)
        if should_skip_node(node.id, 'pin'):
            # Debug message is now provided by should_skip_node function
            return False

        # Pin the chunk on the node
        if node.ip == "127.0.0.1":
            # Local node
            response = requests.post(f"{IPFS_API_URL}/pin/add?arg={chunk.ipfs_hash}",
                                    timeout=TIMEOUTS['PIN_OPERATION'])
        else:
            # Remote node - use shorter timeout for pin operations
            response = requests.post(f"{node.get_api_url()}/pin/add?arg={chunk.ipfs_hash}",
                                    timeout=TIMEOUTS['PIN_OPERATION'])

        if response.status_code == 200:
            # Verify the pin was successful by checking if it exists
            verify_url = f"{node.get_api_url()}/pin/ls?arg={chunk.ipfs_hash}" if node.ip != "127.0.0.1" else f"{IPFS_API_URL}/pin/ls?arg={chunk.ipfs_hash}"
            verify_response = requests.post(verify_url, timeout=TIMEOUTS['QUICK_CHECK'])

            if verify_response.status_code != 200 or chunk.ipfs_hash not in verify_response.text:
                raise Exception("Verification failed: Chunk was not properly pinned")

            # Record the distribution
            ChunkDistribution.objects.create(chunk=chunk, node=node)

            # Update node load
            node.load = min(node.load + 0.2, 100.0)  # Increment but cap at 100%
            node.consecutive_failures = 0  # Reset failure counter on success
            node.save()

            # Record success in our circuit breaker
            record_node_success(node.id, 'pin')

            return True

        # If we get here, the operation failed but didn't throw an exception
        if retry_count < MAX_RETRY_ATTEMPTS:
            time.sleep(1)  # Wait before retrying
            return distribute_chunk_to_node(chunk, node, retry_count + 1)

        # Record the failure in our circuit breaker
        record_node_failure(node.id, 'pin', node.name)

        # Update node failure count
        node.consecutive_failures += 1
        if node.consecutive_failures > 5:
            node.is_active = False
        node.save()

        return False

    except requests.exceptions.Timeout:
        # Handle timeout specifically to provide better error information
        print(f"Timeout distributing chunk {chunk.ipfs_hash} to node {node.name} (ID: {node.id})")

        # Record the failure in our circuit breaker
        record_node_failure(node.id, 'pin', node.name)

        # Update node failure stats
        node.consecutive_failures += 1
        if node.consecutive_failures > 5:
            node.is_active = False
        node.save()

        # Retry logic for timeouts
        if retry_count < MAX_RETRY_ATTEMPTS:
            time.sleep(1)  # Wait before retrying
            return distribute_chunk_to_node(chunk, node, retry_count + 1)

        return False

    except Exception as e:
        print(f"Error distributing chunk {chunk.ipfs_hash} to node {node.name} (ID: {node.id}): {str(e)}")

        # Record the failure in our circuit breaker
        record_node_failure(node.id, 'pin', node.name)

        # Retry logic for recoverable errors
        if retry_count < MAX_RETRY_ATTEMPTS:
            time.sleep(1)  # Wait before retrying
            return distribute_chunk_to_node(chunk, node, retry_count + 1)

        # Mark node as potentially inactive if multiple failures
        node.consecutive_failures += 1
        if node.consecutive_failures > 5:
            node.is_active = False
        node.save()

        return False


# 📌 Upload File with Chunking and Distribution - ENHANCED with better error handling
@login_required
@transaction.atomic
def upload_file(request):
    if request.method == "POST":
        file = request.FILES.get("file")
        if not file:
            return HttpResponse("No file provided", status=400)

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


# 📌 NEW: Enhanced replication function that replaces the legacy method
def ensure_chunk_replication(chunk, min_replicas=DISTRIBUTION_FACTOR):
    """Ensures a chunk is replicated to the desired number of active nodes."""
    # Get current distributions for this chunk
    current_distributions = ChunkDistribution.objects.filter(chunk=chunk).select_related('node')
    active_distributions = [d for d in current_distributions if d.node.is_active]

    # If we already have enough active distributions, we're good

    if success_count > 0:
        return True, f"Added {success_count} new replicas for chunk {chunk.ipfs_hash}"
    elif needed_replicas > 0:
        return False, f"Failed to add {needed_replicas} needed replicas for chunk {chunk.ipfs_hash}"
    else:
        return True, "No additional replicas needed"


# 📌 Download File and Reassemble Chunks - ENHANCED for distributed retrieval
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


# 📌 File Details View - ENHANCED with distribution info
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


# 📌 API Endpoint to trigger redistribution of a file's chunks
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


# 📌 NEW: Health check for file replication
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


def discover_ipfs_nodes():
    """Fetches connected IPFS nodes and adds them to the database."""
    try:
        print(f"Fetching peers from {IPFS_API_URL}/swarm/peers")

        # ✅ Use POST instead of GET
        response = requests.post(f"{IPFS_API_URL}/swarm/peers")

        # ✅ Fix: Ensure response is JSON
        try:
            response_data = response.json()
        except json.JSONDecodeError:
            print("Error: Response from IPFS is not valid JSON.")
            print("Raw Response:", response.text)  # Debugging output
            return "Invalid response from IPFS."

        # ✅ Fix: Check if "Peers" key exists
        if "Peers" not in response_data:
            print("No peers found in response.")
            return "No IPFS peers found."

        peers = response_data["Peers"]
        new_nodes = 0

        for peer in peers:
            ipfs_id = peer["Peer"]
            addr = peer["Addr"]

            # Extract IP from "/ip4/192.168.1.10/tcp/4001"
            parts = addr.split("/")
            if "ip4" in parts:
                ip_index = parts.index("ip4") + 1
                ip_address = parts[ip_index]

                # Ensure node is unique
                if not Node.objects.filter(ipfs_id=ipfs_id).exists():
                    Node.objects.create(ipfs_id=ipfs_id, ip=ip_address, load=0.0)
                    new_nodes += 1

        return f"Discovered {new_nodes} new nodes."

    except Exception as e:
        print("Error discovering nodes:", e)
        return f"Error: {e}"

def is_admin(user):
    return user.is_superuser

@user_passes_test(is_admin, login_url='/login/')
def admin_dashboard(request):
    """Admin dashboard showing storage node statistics."""
    # Comment out discover_ipfs_nodes to prevent auto-discovery
    # discover_ipfs_nodes()

    # Remove any calls to ensure_required_nodes

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


# 📌 API: Manual rebalance of distributions
@user_passes_test(is_admin)
def rebalance_distributions(request):
    """Redistributes chunks to balance load across nodes."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)

    # Find overloaded nodes (load > 80%)
    overloaded_nodes = Node.objects.filter(is_active=True, load__gt=80)

    # Find underutilized nodes (load < 30%)
    underutilized_nodes = Node.objects.filter(is_active=True, load__lt=30)

    redistributions = 0

    if underutilized_nodes.exists():
        for overloaded_node in overloaded_nodes:
            # Get chunks on this overloaded node
            distributions = ChunkDistribution.objects.filter(node=overloaded_node).select_related('chunk')[:50]  # Limit to 50 to avoid overload

            for dist in distributions:
                # Check if this chunk is already on any underutilized node
                for target_node in underutilized_nodes:
                    if not ChunkDistribution.objects.filter(chunk=dist.chunk, node=target_node).exists():
                        # Distribute to this underutilized node
                        if distribute_chunk_to_node(dist.chunk, target_node):
                            redistributions += 1

                            # Adjust loads
                            target_node.load = min(target_node.load + 0.5, 100.0)
                            target_node.save()

                            # Reduce load on overloaded node
                            overloaded_node.load = max(overloaded_node.load - 0.5, 0.0)
                            overloaded_node.save()

                            break

    return JsonResponse({
        'success': True,
        'redistributions': redistributions
    })


# 📌 Test a specific node's health
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


# 📌 NEW: API for checking system status before uploading
@csrf_exempt
def system_status(request):
    try:
        nodes = Node.objects.all()
        active_nodes = sum(1 for node in nodes if node.is_active)
        # Since Node might not have an available_space field, calculate based on load
        available_storage = sum(max(100 - node.load, 0) * 0.1 for node in nodes if node is_active)

        # Check if nodes exist and warn if none do
        node_warning = "" if active_nodes > 0 else "No storage nodes configured! Contact administrator."

        return JsonResponse({
            'active_nodes': active_nodes,
            'available_storage': round(available_storage, 2),  # Simulated GB
            'system_healthy': active_nodes >= 2,  # Consider system healthy if at least 2 nodes are active
            'warning': node_warning
        })
    except Exception as e:
        return JsonResponse({
            'active_nodes': 0,
            'available_storage': 0,
            'system_healthy': False,
            'error': str(e)
        })

# 📌 NEW: Quick node check before file uploads
def quick_node_check(request):
    """Quickly checks if any nodes are responsive before file upload."""
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
            requests.post(f"{node.get_api_url()}/id", timeout=TIMEOUTS['QUICK_CHECK'])
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

# 📌 NEW: Delete File functionality
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

# 📌 NEW: API endpoint to get circuit breaker status
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

# 📌 NEW: API endpoint to reset circuit breaker for a node
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

# 📌 NEW: Debug download view to help debugging
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
    try:
        content_type = detect_content_type(file_obj.fname)
    except Exception:
        pass

    return render(request, "storage/file_debug.html", {
        'file': file_obj,
        'chunk_details': chunk_details,
        'total_chunks': chunks.count(),
        'missing_chunks': sum(1 for detail in chunk_details if detail['available_count'] == 0),
        'file_directly_accessible': file_directly_accessible,
        'content_type': content_type
    })

from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import render, redirect
from django.urls import reverse

def is_admin(user):
    """Check if user is an admin or staff member"""
    return user.is_authenticated and (user.is_superuser or user.is_staff)

@login_required
@user_passes_test(is_admin, login_url='/files/')
def admin_dashboard(request):
    """Admin dashboard view with system statistics"""
    # If user isn't admin, redirect to files page
    if not is_admin(request.user):
        return redirect('file_list')

    # ...existing admin dashboard view code...