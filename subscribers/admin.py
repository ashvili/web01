from django.contrib import admin
from .models import Subscriber, ImportHistory, ImportError

# Register your models here.

@admin.register(Subscriber)
class SubscriberAdmin(admin.ModelAdmin):
    list_display = ('last_name', 'first_name', 'middle_name', 'number', 'imsi', 'is_active', 'created_at')
    list_filter = ('is_active', 'gender', 'import_history', 'created_at')
    search_fields = ('last_name', 'first_name', 'middle_name', 'number', 'imsi', 'address', 'birth_place')
    date_hierarchy = 'created_at'
    ordering = ('-created_at',)
    list_per_page = 50
    readonly_fields = ('created_at', 'updated_at')
    fieldsets = (
        ('Основная информация', {
            'fields': (('last_name', 'first_name', 'middle_name'), ('number', 'imsi'), 'gender')
        }),
        ('Личные данные', {
            'fields': ('birth_date', 'birth_place', 'address')
        }),
        ('Дополнительная информация', {
            'fields': ('memo1', 'memo2', 'email')
        }),
        ('Служебная информация', {
            'fields': ('original_id', 'is_active', 'import_history', 'created_at', 'updated_at')
        }),
    )

@admin.register(ImportHistory)
class ImportHistoryAdmin(admin.ModelAdmin):
    list_display = ('id', 'file_name', 'created_at', 'status', 'records_count', 'records_created', 'records_failed')
    list_filter = ('status', 'created_at')
    search_fields = ('file_name',)
    readonly_fields = ('file_name', 'file_size', 'created_at', 'created_by', 'status', 
                      'records_count', 'records_created', 'records_failed', 'archive_table_name', 'error_message')
    fieldsets = (
        ('Основная информация', {
            'fields': ('file_name', 'file_size', 'created_at', 'created_by', 'status')
        }),
        ('Результаты импорта', {
            'fields': ('records_count', 'records_created', 'records_failed', 'archive_table_name', 'error_message')
        }),
    )
    
    def has_add_permission(self, request):
        return False  # Запрещаем ручное создание записей
    
    def has_delete_permission(self, request, obj=None):
        # Разрешаем удаление только для суперпользователей
        return request.user.is_superuser

@admin.register(ImportError)
class ImportErrorAdmin(admin.ModelAdmin):
    list_display = ('import_history', 'row_index', 'message', 'created_at')
    list_filter = ('import_history', 'created_at')
    search_fields = ('message', 'raw_data')
    readonly_fields = ('import_history', 'row_index', 'message', 'raw_data', 'created_at')
    ordering = ('-created_at',)
    list_per_page = 100
    
    fieldsets = (
        ('Основная информация', {
            'fields': ('import_history', 'row_index', 'created_at')
        }),
        ('Детали ошибки', {
            'fields': ('message', 'raw_data')
        }),
    )
    
    def has_add_permission(self, request):
        return False  # Запрещаем ручное создание записей
    
    def has_delete_permission(self, request, obj=None):
        # Разрешаем удаление только для суперпользователей
        return request.user.is_superuser
