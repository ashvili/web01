from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from django.contrib.auth.models import User, Group
from .models import UserProfile

# Register your models here.

class UserProfileInline(admin.StackedInline):
    model = UserProfile
    can_delete = False
    verbose_name_plural = 'Профиль'
    fk_name = 'user'
    fieldsets = (
        (None, {'fields': ('department', 'position', 'phone_number')}),
        ('Настройки интерфейса', {'fields': ('items_per_page', 'theme')}),
        ('Двухфакторная аутентификация', {'fields': ('totp_enabled', 'totp_secret')}),
        ('Права доступа', {'fields': ('can_import_data', 'can_export_data', 'can_view_logs')}),
    )
    readonly_fields = ('can_import_data', 'can_export_data', 'can_view_logs')

class CustomUserAdmin(UserAdmin):
    inlines = (UserProfileInline,)
    list_display = ('username', 'email', 'first_name', 'last_name', 'is_staff', 'get_department')
    list_select_related = ('profile',)
    list_filter = ('is_staff', 'is_superuser', 'is_active', 'groups')
    
    def get_department(self, instance):
        return instance.profile.department
    get_department.short_description = 'Отдел'
    
    def get_inline_instances(self, request, obj=None):
        if not obj:
            return list()
        return super(CustomUserAdmin, self).get_inline_instances(request, obj)
    
    def save_formset(self, request, form, formset, change):
        """Обрабатывает сохранение инлайновых форм и обновляет права пользователя"""
        instances = formset.save()
        
        # Обновляем права пользователя при изменении профиля
        for instance in instances:
            if isinstance(instance, UserProfile):
                instance.update_permissions()
        return instances

# Перерегистрация модели User с нашей версией
admin.site.unregister(User)
admin.site.register(User, CustomUserAdmin)

# Регистрация модели группы с более подробным отображением
class CustomGroupAdmin(admin.ModelAdmin):
    list_display = ('name', 'get_permissions')
    filter_horizontal = ('permissions',)
    
    def get_permissions(self, obj):
        return ", ".join([p.name for p in obj.permissions.all()[:5]])
    get_permissions.short_description = 'Разрешения'

admin.site.unregister(Group)
admin.site.register(Group, CustomGroupAdmin)
