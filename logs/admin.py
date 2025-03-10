from django.contrib import admin
from .models import UserActionLog

# Register your models here.

@admin.register(UserActionLog)
class UserActionLogAdmin(admin.ModelAdmin):
    list_display = ('action_type', 'user', 'action_time', 'ip_address', 'content_type', 'object_id')
    list_filter = ('action_type', 'user', 'action_time', 'content_type')
    search_fields = ('user__username', 'ip_address', 'object_id')
    date_hierarchy = 'action_time'
    readonly_fields = ('user', 'action_type', 'action_time', 'ip_address', 'user_agent', 
                      'content_type', 'object_id', 'additional_data')
    ordering = ('-action_time',)
    list_per_page = 50
    
    def has_add_permission(self, request):
        return False
    
    def has_change_permission(self, request, obj=None):
        return False
    
    def has_delete_permission(self, request, obj=None):
        # Только суперпользователи могут удалять логи
        return request.user.is_superuser
