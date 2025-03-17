"""Constants used throughout the storage application."""

# IPFS settings
IPFS_API_URL = "http://127.0.0.1:5001/api/v0"

# Storage settings
CHUNK_SIZE = 1024 * 1024  # 1MB chunks
DISTRIBUTION_FACTOR = 2  # Number of nodes to distribute each chunk to

# Timeout thresholds for different operations (in seconds)
TIMEOUTS = {
    'QUICK_CHECK': 2,      # Fast health check timeout
    'PIN_OPERATION': 10,   # Timeout for IPFS pin operations
    'UPLOAD_OPERATION': 15  # Timeout for file upload operations
}

# Blacklisted IP addresses
BLACKLISTED_IPS = ['103.143.148.185', '1.36.226.78', '104.168.82.126']
