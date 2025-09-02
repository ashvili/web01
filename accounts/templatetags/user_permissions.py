from django import template
from accounts.utils import is_admin, is_user1, is_user2, can_view_logs, can_import_data, can_export_data, can_view_imsi

register = template.Library()

@register.filter
def is_admin_user(user):
    """Template filter для проверки, является ли пользователь администратором"""
    return is_admin(user)

@register.filter
def is_user1_user(user):
    """Template filter для проверки, является ли пользователь пользователем 1 уровня"""
    return is_user1(user)

@register.filter
def is_user2_user(user):
    """Template filter для проверки, является ли пользователь пользователем 2 уровня"""
    return is_user2(user)

@register.filter
def can_view_logs_user(user):
    """Template filter для проверки, может ли пользователь просматривать логи"""
    return can_view_logs(user)

@register.filter
def can_import_data_user(user):
    """Template filter для проверки, может ли пользователь импортировать данные"""
    return can_import_data(user)

@register.filter
def can_export_data_user(user):
    """Template filter для проверки, может ли пользователь экспортировать данные"""
    return can_export_data(user)

@register.filter
def can_view_imsi_user(user):
    """Template filter для проверки, может ли пользователь видеть поле IMSI"""
    return can_view_imsi(user)
