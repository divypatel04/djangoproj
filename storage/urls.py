from django.urls import path
from .views import (
    index, user_login, user_register, user_logout,
    upload_file, file_list, download_file, admin_dashboard,
    file_details, redistribute_file, rebalance_distributions,
    system_health_check, test_node, system_status, quick_node_check,
    delete_file, circuit_breaker_status, reset_circuit_breaker, debug_download,
    node_diagnostic, remove_nodes
)

urlpatterns = [
    path("", index, name="index"),
    path("login/", user_login, name="login"),
    path("register/", user_register, name="register"),
    path("logout/", user_logout, name="logout"),
    path("upload/", upload_file, name="upload_file"),
    path("files/", file_list, name="file_list"),
    path("file/<int:file_id>/", file_details, name="file_details"),
    path("download/<str:ipfs_hash>/", download_file, name="download_file"),
    path("admin-dashboard/", admin_dashboard, name="admin_dashboard"),

    # Distribution management API endpoints
    path("api/redistribute-file/<int:file_id>/", redistribute_file, name="redistribute_file"),
    path("api/rebalance-distributions/", rebalance_distributions, name="rebalance_distributions"),
    path("api/system-health-check/", system_health_check, name="system_health_check"),
    path("api/test-node/<int:node_id>/", test_node, name="test_node"),

    # New API endpoints for upload reliability
    path("api/system-status/", system_status, name="system_status"),
    path("api/quick-node-check/", quick_node_check, name="quick_node_check"),

    # File delete endpoint
    path("delete-file/<int:file_id>/", delete_file, name="delete_file"),

    # Circuit breaker management endpoints
    path("api/circuit-breaker/status/", circuit_breaker_status, name="circuit_breaker_status"),
    path("api/circuit-breaker/reset/", reset_circuit_breaker, name="reset_all_circuit_breakers"),
    path("api/circuit-breaker/reset/<int:node_id>/", reset_circuit_breaker, name="reset_node_circuit_breaker"),

    # Add this new path:
    path("file-debug/<int:file_id>/", debug_download, name="debug_download"),

    # Add the new URL pattern for node diagnostics
    path("node-diagnostic/", node_diagnostic, name="node_diagnostic"),

    # Add the new URL pattern for node management
    path("manage-nodes/", remove_nodes, name="remove_nodes"),
]
