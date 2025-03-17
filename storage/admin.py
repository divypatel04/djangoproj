from django.contrib import admin
from django.utils.html import format_html, mark_safe
from django.db.models import Count
from .models import File, Node, FileChunk, ChunkDistribution

# Custom filter for distribution count
class DistributionCountFilter(admin.SimpleListFilter):
    title = 'distribution count'
    parameter_name = 'distribution_count'

    def lookups(self, request, model_admin):
        return (
            ('0', 'No distributions'),
            ('1', 'Single node (vulnerable)'),
            ('2', 'Two nodes (minimum)'),
            ('3+', 'Three or more (robust)'),
        )

    def queryset(self, request, queryset):
        if self.value() == '0':
            return queryset.annotate(dist_count=Count('distributions')).filter(dist_count=0)
        if self.value() == '1':
            return queryset.annotate(dist_count=Count('distributions')).filter(dist_count=1)
        if self.value() == '2':
            return queryset.annotate(dist_count=Count('distributions')).filter(dist_count=2)
        if self.value() == '3+':
            return queryset.annotate(dist_count=Count('distributions')).filter(dist_count__gte=3)
        return queryset

# Custom filter for node health
class NodeHealthFilter(admin.SimpleListFilter):
    title = 'node health'
    parameter_name = 'node_health'

    def lookups(self, request, model_admin):
        return (
            ('healthy', 'Healthy (no failures)'),
            ('warning', 'Warning (1-3 failures)'),
            ('critical', 'Critical (4+ failures)'),
        )

    def queryset(self, request, queryset):
        if self.value() == 'healthy':
            return queryset.filter(consecutive_failures=0)
        if self.value() == 'warning':
            return queryset.filter(consecutive_failures__gt=0, consecutive_failures__lt=4)
        if self.value() == 'critical':
            return queryset.filter(consecutive_failures__gte=4)
        return queryset

# Admin actions
def fix_chunk_distribution(modeladmin, request, queryset):
    """Ensures chunks have the minimum required distribution factor."""
    from .views import ensure_chunk_replication, DISTRIBUTION_FACTOR

    fixed_count = 0
    for chunk in queryset:
        success, _ = ensure_chunk_replication(chunk, DISTRIBUTION_FACTOR)
        if success:
            fixed_count += 1

    modeladmin.message_user(
        request,
        "Successfully fixed distribution for {} out of {} chunks.".format(fixed_count, queryset.count())
    )
fix_chunk_distribution.short_description = "Fix distribution of selected chunks"

def reset_node_failures(modeladmin, request, queryset):
    """Reset consecutive failures counter and mark nodes as active."""
    updated = queryset.update(consecutive_failures=0, is_active=True)
    modeladmin.message_user(request, "Reset {} nodes to active state with no failures.".format(updated))
reset_node_failures.short_description = "Reset failure count and activate nodes"

def test_node_connection(modeladmin, request, queryset):
    """Test if nodes are reachable and update their status."""
    import requests

    success_count = 0
    for node in queryset:
        try:
            response = requests.post(f"{node.get_api_url()}/id", timeout=5)
            if response.status_code == 200:
                node.is_active = True
                node.consecutive_failures = 0
                node.save()
                success_count += 1
        except Exception:
            node.is_active = False
            node.consecutive_failures += 1
            node.save()

    modeladmin.message_user(
        request,
        "Successfully connected to {} out of {} nodes.".format(success_count, queryset.count())
    )
test_node_connection.short_description = "Test connection to selected nodes"

class FileChunkInline(admin.TabularInline):
    model = FileChunk
    extra = 0
    readonly_fields = ['ipfs_hash', 'chunk_index', 'size']
    can_delete = False
    show_change_link = True

@admin.register(File)
class FileAdmin(admin.ModelAdmin):
    list_display = ['fname', 'user', 'size_display', 'uploaded_at', 'distribution_status']
    search_fields = ['fname', 'user__username']
    readonly_fields = ['ipfs_hash', 'checksum']
    inlines = [FileChunkInline]

    def size_display(self, obj):
        """Display file size in human-readable format."""
        size = obj.size
        if size < 1024:
            return f"{size} B"
        elif size < 1024 * 1024:
            return f"{size/1024:.1f} KB"
        elif size < 1024 * 1024 * 1024:
            return f"{size/(1024*1024):.1f} MB"
        else:
            return f"{size/(1024*1024*1024):.1f} GB"

    size_display.short_description = "Size"

    def distribution_status(self, obj):
        """Display distribution status with color coding."""
        status = obj.get_distribution_status()
        node_count = status.get('node_count', 0)

        if node_count >= 2:
            return format_html('<span style="color: green;">Good ({} nodes)</span>', node_count)
        elif node_count == 1:
            return format_html('<span style="color: orange;">Limited (1 node)</span>')
        else:
            return format_html('<span style="color: red;">None (0 nodes)</span>')

    distribution_status.short_description = "Distribution"

@admin.register(FileChunk)
class FileChunkAdmin(admin.ModelAdmin):
    list_display = ['chunk_index', 'file_name', 'ipfs_hash', 'size_display', 'node_count']
    search_fields = ['file__fname', 'ipfs_hash']
    list_filter = ['file']

    def file_name(self, obj):
        return obj.file.fname

    file_name.short_description = "File"

    def size_display(self, obj):
        """Display chunk size in human-readable format."""
        size = obj.size or 0
        if size < 1024:
            return f"{size} B"
        elif size < 1024 * 1024:
            return f"{size/1024:.1f} KB"
        else:
            return f"{size/(1024*1024):.1f} MB"  # Fixed: removed extra colon

    size_display.short_description = "Size"

    def node_count(self, obj):
        """Display the number of nodes this chunk is distributed to."""
        count = obj.distributions.count()
        if count >= 2:
            return format_html('<span style="color: green;">{}</span>', count)
        elif count == 1:
            return format_html('<span style="color: orange;">1</span>')
        else:
            return format_html('<span style="color: red;">0</span>')

    node_count.short_description = "Nodes"

@admin.register(Node)
class NodeAdmin(admin.ModelAdmin):
    list_display = ['name', 'ip', 'port', 'status_display', 'load_formatted', 'chunk_count', 'last_seen']
    search_fields = ['name', 'ip', 'ipfs_id']
    list_filter = ['is_active']
    readonly_fields = ['last_seen', 'consecutive_failures']
    actions = [reset_node_failures, test_node_connection]

    def status_display(self, obj):
        """Display node status with color coding."""
        if obj.is_active:
            return mark_safe('<span style="color: green;">Active</span>')
        else:
            return mark_safe('<span style="color: red;">Inactive</span>')

    status_display.short_description = "Status"

    def load_formatted(self, obj):
        """Display load with color coding."""
        load = obj.load
        if load < 30:
            return mark_safe('<span style="color: green;">{:.1f}%</span>'.format(load))
        elif load < 70:
            return mark_safe('<span style="color: orange;">{:.1f}%</span>'.format(load))
        else:
            return mark_safe('<span style="color: red;">{:.1f}%</span>'.format(load))

    load_formatted.short_description = "Load"
    load_formatted.admin_order_field = 'load'

    def chunk_count(self, obj):
        """Display the number of chunks stored on this node."""
        return obj.chunk_distributions.count()

    chunk_count.short_description = "Chunks"

@admin.register(ChunkDistribution)
class ChunkDistributionAdmin(admin.ModelAdmin):
    list_display = ['chunk_display', 'node_display', 'created_at']
    search_fields = ['chunk__file__fname', 'node__name']
    list_filter = ['node', 'created_at']

    def chunk_display(self, obj):
        chunk_text = f"Chunk {obj.chunk.chunk_index} of {obj.chunk.file.fname}"
        return chunk_text

    chunk_display.short_description = "Chunk"

    def node_display(self, obj):
        if obj.node.is_active:
            return mark_safe('<span style="color: green;">{}</span>'.format(obj.node.name))
        else:
            return mark_safe('<span style="color: red;">{}</span>'.format(obj.node.name))

    node_display.short_description = "Node"