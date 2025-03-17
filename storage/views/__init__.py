"""
Views package for the storage application.
This file imports and exposes all views to maintain backward compatibility.
"""

# Import views from specific modules to avoid circular imports
from .base_views import (
    index, user_login, user_register, user_logout,
    upload_file, file_list, download_file,
    file_details, redistribute_file, system_health_check,
    system_status, quick_node_check, delete_file,
    debug_download, discover_ipfs_nodes, is_admin
    # Remove ensure_chunk_replication from here
)

from .admin_views import (
    admin_dashboard, rebalance_distributions, test_node,
    circuit_breaker_status, reset_circuit_breaker,
    node_diagnostic, remove_nodes
)

# Import utility functions - moved to utils.py
from .utils import (
    distribute_chunk_to_node, should_skip_node,
    record_node_failure, record_node_success,
    NODE_FAILURE_CACHE, ensure_chunk_replication  # Add this import
)
