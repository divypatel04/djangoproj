import mimetypes
import os
import logging
import hashlib
from io import BytesIO

logger = logging.getLogger(__name__)

def detect_content_type(filename):
    """
    Detect the content type of a file based on its extension
    """
    content_type, encoding = mimetypes.guess_type(filename)

    # Common types that might not be properly detected
    extensions_map = {
        '.txt': 'text/plain',
        '.csv': 'text/csv',
        '.pdf': 'application/pdf',
        '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
        '.gif': 'image/gif',
        '.svg': 'image/svg+xml',
        '.mp4': 'video/mp4',
        '.mp3': 'audio/mpeg',
        '.wav': 'audio/wav',
        '.html': 'text/html',
        '.xml': 'application/xml',
        '.json': 'application/json',
        '.zip': 'application/zip',
        '.md': 'text/markdown',
    }

    if not content_type:
        # Try to get extension and map to content type
        ext = os.path.splitext(filename)[1].lower()
        if ext in extensions_map:
            content_type = extensions_map[ext]
        else:
            # Default fallback
            content_type = 'application/octet-stream'

    logger.debug(f"Detected content type for {filename}: {content_type}")
    return content_type

def verify_file_integrity(binary_data, expected_checksum=None):
    """
    Verify the integrity of binary data with a checksum
    """
    calculated_checksum = hashlib.sha256(binary_data).hexdigest()

    if expected_checksum:
        is_valid = calculated_checksum == expected_checksum
        if not is_valid:
            logger.warning(f"Checksum verification failed: expected {expected_checksum}, got {calculated_checksum}")
        return is_valid, calculated_checksum

    return True, calculated_checksum

def estimate_file_type(data_sample):
    """
    Try to detect the file type from the first few bytes (magic numbers)
    """
    # Dictionary of file signatures
    signatures = {
        b'\xFF\xD8\xFF': 'image/jpeg',
        b'\x89\x50\x4E\x47\x0D\x0A\x1A\x0A': 'image/png',
        b'\x47\x49\x46\x38': 'image/gif',
        b'\x25\x50\x44\x46': 'application/pdf',
        b'\x50\x4B\x03\x04': 'application/zip',  # also docx, xlsx, etc.
        b'\x3C\x3F\x78\x6D\x6C': 'application/xml',
        b'\x7B': 'application/json',  # JSON starting with '{'
        b'\x1F\x8B': 'application/gzip',
    }

    if not data_sample or len(data_sample) < 4:
        return None

    # Check each signature
    for signature, mime_type in signatures.items():
        if data_sample.startswith(signature):
            return mime_type

    # Check if it's likely text
    text_chars = set(bytes(range(32, 127)) + b'\n\r\t\b')
    is_likely_text = all(byte in text_chars for byte in data_sample[:min(len(data_sample), 1000)])

    if is_likely_text:
        # Check for common formats
        if b'<!DOCTYPE html>' in data_sample or b'<html' in data_sample:
            return 'text/html'
        if b'<?xml' in data_sample:
            return 'application/xml'
        return 'text/plain'

    return None
