# IPFS Distributed Storage System

This Django application provides a distributed file storage system using IPFS (InterPlanetary File System). It allows users to upload files, which are then chunked and distributed across multiple IPFS nodes for redundancy and reliability.

## Features

- User authentication and file management
- File chunking for optimized storage and distribution
- Redundant storage across multiple IPFS nodes
- Automatic rebalancing of chunk distribution
- Health monitoring and self-healing capabilities
- Admin dashboard for system oversight

## Admin Interface

The admin interface provides comprehensive management capabilities:

1. **Django Admin**: Enhanced admin views for Files, Chunks, Nodes, and Distributions
2. **Custom Dashboard**: Visual dashboard for monitoring system health
3. **Health Check**: Manual and automated health checking with repair capabilities
4. **Node Management**: Test and manage IPFS node connections

## Automated Maintenance

The system includes a management command for automated health checks and repairs. This can be scheduled using cron (Linux/Mac) or Task Scheduler (Windows).

### Using the health check command

```bash
# Check system health without making changes
python manage.py check_system_health --dry-run

# Check system health and fix issues
python manage.py check_system_health
```

### Scheduling regular health checks

#### Linux/Mac (cron)

Add to crontab with `crontab -e`:

```
# Run health check every 6 hours
0 */6 * * * cd /path/to/project && /path/to/python manage.py check_system_health >> /path/to/health_log.log 2>&1
```

#### Windows (Task Scheduler)

1. Open Task Scheduler
2. Create a new basic task
3. Set the trigger (e.g., daily)
4. Set the action to run a program:
   - Program/script: `C:\path\to\python.exe`
   - Arguments: `C:\path\to\project\manage.py check_system_health`
   - Start in: `C:\path\to\project\`

## Using the System

1. Register or log in to the system
2. Upload files via the upload page
3. View your files in the file list
4. Download files when needed
5. View file details including distribution status
6. Administrators can access the admin dashboard to monitor system health

## Technical Implementation

- Files are split into 1MB chunks
- Each chunk is stored on IPFS with a unique hash
- Chunks are distributed to multiple nodes for redundancy
- The system tracks node health and redistributes chunks as needed
- Node load is balanced to prevent any single node from being overwhelmed
