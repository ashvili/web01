from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from django.contrib.auth.models import User
from .models import UserProfile

# Register your models here.

class UserProfileInline(admin.StackedInline):
    model = UserProfile
    can_delete = False
    verbose_name_plural = 'Профиль'
    fk_name = 'user'

class CustomUserAdmin(UserAdmin):
    inlines = (UserProfileInline,)
    list_display = ('username', 'email', 'first_name', 'last_name', 'is_staff', 'get_department')
    list_select_related = ('profile',)
    
    def get_department(self, instance):
        return instance.profile.department
    get_department.short_description = 'Отдел'
    
    def get_inline_instances(self, request, obj=None):
        if not obj:
            return list()
        return super(CustomUserAdmin, self).get_inline_instances(request, obj)

# Перерегистрация модели User с нашей версией
admin.site.unregister(User)
admin.site.register(User, CustomUserAdmin)
