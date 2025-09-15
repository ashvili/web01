from django.contrib import admin
from .models import Subscriber, ImportHistory, ImportError
from accounts.utils import can_view_imsi

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
    
    def get_list_display(self, request):
        """Динамически изменяет отображаемые колонки в зависимости от разрешений пользователя"""
        list_display = list(super().get_list_display(request))
        if not can_view_imsi(request.user):
            # Удаляем IMSI из списка отображаемых колонок
            if 'imsi' in list_display:
                list_display.remove('imsi')
        return list_display
    
    def get_search_fields(self, request):
        """Динамически изменяет поля поиска в зависимости от разрешений пользователя"""
        search_fields = list(super().get_search_fields(request))
        if not can_view_imsi(request.user):
            # Удаляем IMSI из полей поиска
            if 'imsi' in search_fields:
                search_fields.remove('imsi')
        return search_fields
    
    def get_fieldsets(self, request, obj=None):
        """Динамически изменяет поля в форме в зависимости от разрешений пользователя"""
        fieldsets = list(super().get_fieldsets(request, obj))
        if not can_view_imsi(request.user):
            # Удаляем IMSI из полей формы
            for section_name, section_data in fieldsets:
                if 'fields' in section_data:
                    fields = list(section_data['fields'])
                    # Ищем и удаляем IMSI из кортежей полей
                    new_fields = []
                    for field_group in fields:
                        if isinstance(field_group, tuple):
                            # Если это кортеж полей, удаляем IMSI из него
                            new_group = tuple(f for f in field_group if f != 'imsi')
                            if new_group:  # Добавляем только если остались поля
                                new_fields.append(new_group)
                        elif field_group != 'imsi':
                            # Если это отдельное поле и не IMSI, добавляем его
                            new_fields.append(field_group)
                    section_data['fields'] = tuple(new_fields)
        return fieldsets

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
