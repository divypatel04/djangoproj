from django.test import TestCase, Client
from django.contrib.auth.models import User
from django.urls import reverse
from .models import Node, File, FileChunk, ChunkDistribution
from unittest.mock import patch, MagicMock
import requests

class NodeConnectionTests(TestCase):
    def setUp(self):
        # Create a test user
        self.user = User.objects.create_user(username='testuser', password='password')

        # Create some test nodes
        self.node1 = Node.objects.create(
            name="TestNode1",
            ipfs_id="test1",
            ip="192.168.1.1",
            load=10.0,
            is_active=True
        )

        self.node2 = Node.objects.create(
            name="TestNode2",
            ipfs_id="test2",
            ip="192.168.1.2",
            load=20.0,
            is_active=True
        )

        # Create a local node
        self.local_node = Node.objects.create(
            name="LocalNode",
            ipfs_id="local",
            ip="127.0.0.1",
            load=5.0,
            is_active=True
        )

        # Client for making requests
        self.client = Client()

    def test_quick_node_check_endpoint(self):
        """Test that the quick node check endpoint works."""
        # Login the test user
        self.client.login(username='testuser', password='password')

        # Mock the requests.post to simulate different node responses
        with patch('requests.post') as mock_post:
            # Set up the mock to return success for LocalNode but timeout for others
            def side_effect(*args, **kwargs):
                if "127.0.0.1" in args[0]:
                    mock_response = MagicMock()
                    mock_response.status_code = 200
                    return mock_response
                else:
                    raise requests.exceptions.Timeout("Connection timed out")

            mock_post.side_effect = side_effect

            # Test the endpoint
            response = self.client.get(reverse('quick_node_check'))
            self.assertEqual(response.status_code, 200)

            # At least one node (LocalNode) should be responsive
            data = response.json()
            self.assertFalse(data['all_nodes_ok'])
            self.assertTrue(data['any_nodes_ok'])

    def test_system_status_endpoint(self):
        """Test the system status endpoint."""
        # Login the test user
        self.client.login(username='testuser', password='password')

        # Mock the requests.post to simulate responses
        with patch('requests.post') as mock_post:
            # Set up mock to return success
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_post.return_value = mock_response

            # Test the endpoint
            response = self.client.get(reverse('system_status'))
            self.assertEqual(response.status_code, 200)

            # Check the response data
            data = response.json()
            self.assertEqual(data['active_nodes'], 3)  # All our test nodes are active
            self.assertEqual(data['recent_failures'], 0)  # No failures yet

    @patch('requests.post')
    def test_node_failure_handling(self, mock_post):
        """Test that node failures are properly tracked."""
        # Configure the mock to simulate a failing node
        mock_post.side_effect = requests.exceptions.Timeout("Connection timed out")

        # Login and access the test node endpoint
        self.client.login(username='testuser', password='password')
        response = self.client.post(reverse('test_node', args=[self.node1.id]))

        # Refresh the node from the database
        self.node1.refresh_from_db()

        # Check that failures were recorded
        self.assertTrue(self.node1.consecutive_failures > 0)

        # Check response
        data = response.json()
        self.assertFalse(data['success'])

class FileOperationsTests(TestCase):
    def setUp(self):
        # Create a test user
        self.user = User.objects.create_user(username='testuser', password='password')
        self.client = Client()
        self.client.login(username='testuser', password='password')

        # Create a test file
        self.file = File.objects.create(
            user=self.user,
            fname="testfile.txt",
            ipfs_hash="test_hash_123",
            size=1024,
            checksum="test_checksum_123"
        )

        # Create a test chunk
        self.chunk = FileChunk.objects.create(
            file=self.file,
            chunk_index=0,
            ipfs_hash="test_chunk_hash_123",
            size=1024
        )

        # Create a test node
        self.node = Node.objects.create(
            name="TestNode1",
            ipfs_id="test1",
            ip="192.168.1.1",
            load=10.0,
            is_active=True
        )

        # Create a distribution
        self.distribution = ChunkDistribution.objects.create(
            chunk=self.chunk,
            node=self.node
        )

    @patch('requests.post')
    def test_delete_file(self, mock_post):
        """Test that a file can be deleted with all its chunks and distributions."""
        # Set up the mock to return success
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        # Get the count of files, chunks, and distributions before deletion
        files_before = File.objects.count()
        chunks_before = FileChunk.objects.count()
        distributions_before = ChunkDistribution.objects.count()

        # Delete the file
        response = self.client.post(reverse('delete_file', args=[self.file.id]))
        self.assertEqual(response.status_code, 200)

        # Verify the counts after deletion
        files_after = File.objects.count()
        chunks_after = FileChunk.objects.count()
        distributions_after = ChunkDistribution.objects.count()

        self.assertEqual(files_after, files_before - 1)
        self.assertEqual(chunks_after, chunks_before - 1)
        self.assertEqual(distributions_after, distributions_before - 1)

        # Verify the response data
        data = response.json()
        self.assertTrue(data['success'])
        self.assertIn('successfully deleted', data['message'])

    def test_delete_nonexistent_file(self):
        """Test deletion of a file that doesn't exist."""
        response = self.client.post(reverse('delete_file', args=[9999]))  # Non-existent ID
        self.assertEqual(response.status_code, 404)  # Should return 404 Not Found

    def test_delete_file_wrong_user(self):
        """Test that a user cannot delete another user's file."""
        # Create another user
        other_user = User.objects.create_user(username='otheruser', password='password')

        # Create a file owned by the other user
        other_file = File.objects.create(
            user=other_user,
            fname="otherfile.txt",
            ipfs_hash="other_hash_123",
            size=2048,
            checksum="other_checksum_123"
        )

        # Try to delete the other user's file
        response = self.client.post(reverse('delete_file', args=[other_file.id]))
        self.assertEqual(response.status_code, 404)  # Should fail with 404

        # Verify the file still exists
        self.assertTrue(File.objects.filter(id=other_file.id).exists())
