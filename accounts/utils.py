from django.contrib.auth.models import User, Permission
from django.core.exceptions import PermissionDenied
from functools import wraps

def user_has_permission(user, permission_codename):
    """
    Проверяет, имеет ли пользователь указанное разрешение
    
    Args:
        user: Экземпляр пользователя
        permission_codename: Кодовое имя разрешения
        
    Returns:
        bool: True, если пользователь имеет данное разрешение
    """
    if user.is_superuser:
        return True
    
    # Проверяем по моделям разрешений Django
    if user.has_perm(permission_codename):
        return True
    
    # Проверяем по профилю пользователя
    if hasattr(user, "profile"):
        if permission_codename == "can_import_data" and user.profile.can_import_data:
            return True
        elif permission_codename == "can_export_data" and user.profile.can_export_data:
            return True
        elif permission_codename == "can_view_logs" and user.profile.can_view_logs:
            return True
    
    return False

def can_view_imsi(user):
    """
    Проверяет, может ли пользователь видеть поле IMSI
    
    Args:
        user: Экземпляр пользователя
        
    Returns:
        bool: True, если пользователь может видеть поле IMSI
    """
    if user.is_superuser:
        return True
    
    if hasattr(user, "profile"):
        return user.profile.user_type != 2
    
    return False

def can_import_data(user):
    """
    Проверяет, может ли пользователь импортировать данные
    
    Args:
        user: Экземпляр пользователя
        
    Returns:
        bool: True, если пользователь может импортировать данные
    """
    if user.is_superuser:
        return True
    
    if hasattr(user, "profile"):
        return user.profile.user_type == 0
    
    return False

def can_view_history(user):
    """
    Проверяет, может ли пользователь просматривать историю импорта
    
    Args:
        user: Экземпляр пользователя
        
    Returns:
        bool: True, если пользователь может просматривать историю импорта
    """
    if user.is_superuser:
        return True
    
    if hasattr(user, "profile"):
        return user.profile.user_type == 0
    
    return False

def imsi_required(view_func):
    """
    Декоратор для проверки права просмотра IMSI
    """
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        if not can_view_imsi(request.user):
            raise PermissionDenied("У вас нет прав для просмотра IMSI")
        return view_func(request, *args, **kwargs)
    return _wrapped_view

def import_required(view_func):
    """
    Декоратор для проверки права импорта данных
    """
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        if not can_import_data(request.user):
            raise PermissionDenied("У вас нет прав для импорта данных")
        return view_func(request, *args, **kwargs)
    return _wrapped_view

def history_required(view_func):
    """
    Декоратор для проверки права просмотра истории импорта
    """
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        if not can_view_history(request.user):
            raise PermissionDenied("У вас нет прав для просмотра истории импорта")
        return view_func(request, *args, **kwargs)
    return _wrapped_view