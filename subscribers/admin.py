from django.contrib import admin
from .models import Subscriber, ImportHistory

# Register your models here.

@admin.register(Subscriber)
class SubscriberAdmin(admin.ModelAdmin):
    list_display = ('last_name', 'first_name', 'middle_name', 'phone_number', 'email', 'is_active', 'created_at')
    list_filter = ('is_active', 'gender', 'import_id')
    search_fields = ('last_name', 'first_name', 'middle_name', 'phone_number', 'email', 'address')
    date_hierarchy = 'created_at'
    ordering = ('-created_at',)
    list_per_page = 25

@admin.register(ImportHistory)
class ImportHistoryAdmin(admin.ModelAdmin):
    list_display = ('id', 'file_name', 'records_count', 'file_size', 'created_at', 'created_by')
    list_filter = ('created_at', 'created_by')
    search_fields = ('file_name', 'created_by__username')
    date_hierarchy = 'created_at'
    ordering = ('-created_at',)
    list_per_page = 25
