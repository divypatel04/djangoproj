"""API views for the storage application."""

import time
import requests
from django.http import JsonResponse
from django.contrib.auth.decorators import user_passes_test

from ..models import Node
from .base_views import TIMEOUTS, is_admin
from .utils import should_skip_node, record_node_failure, record_node_success

def system_status(request):
    """Returns information about node health and system status."""
    active_nodes = Node.objects.filter(is_active=True).count()

    # Get nodes with recent failures
    nodes_with_failures = Node.objects.filter(consecutive_failures__gt=0).count()

    # Simulate response time by actually testing a few nodes
    response_times = []
    for node in Node.objects.filter(is_active=True)[:3]:  # Test max 3 nodes
        try:
            start_time = time.time()
            requests.post(f"{node.get_api_url()}/id", timeout=TIMEOUTS['QUICK_CHECK'])
            response_time = (time.time() - start_time) * 1000  # convert to ms
            response_times.append(response_time)
        except Exception:
            # If this fails, don't include in avg but count as failure
            pass

    avg_response_time = sum(response_times) / len(response_times) if response_times else 500.0  # Default 500ms if no data

    return JsonResponse({
        'active_nodes': active_nodes,
        'recent_failures': nodes_with_failures,
        'avg_response_time': avg_response_time
    })

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
