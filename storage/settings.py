"""
Custom settings for the IPFS storage application.
"""

# IPFS Connection Settings
IPFS_CONNECTION = {
    # Base IPFS API URL
    'API_URL': 'http://127.0.0.1:5001/api/v0',

    # Connection timeouts (in seconds)
    'TIMEOUTS': {
        # Timeout for quick operations like checking node status
        'QUICK': 5,

        # Timeout for distribution operations (pinning)
        'DISTRIBUTION': 8,

        # Timeout for upload operations
        'UPLOAD': 15,

        # Timeout for download operations
        'DOWNLOAD': 20,
    },

    # Number of times to retry failed operations
    'MAX_RETRIES': 3,

    # How many nodes should each chunk be distributed to
    'DISTRIBUTION_FACTOR': 2,

    # Chunk size in bytes (1MB)
    'CHUNK_SIZE': 1024 * 1024,
}

# Cache Settings
CACHE_TTL = {
    # How long to cache node status in seconds
    'NODE_STATUS': 60,

    # How long to cache chunk distribution info in seconds
    'CHUNK_DISTRIBUTION': 300,
}

# Node Health Settings
NODE_HEALTH = {
    # Maximum consecutive failures before marking a node as inactive
    'MAX_FAILURES': 5,

    # Minimum successful operations required to clear failure count
    'SUCCESS_THRESHOLD': 3,
}
