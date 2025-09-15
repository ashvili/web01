from django.contrib.auth.models import User, Permission
from django.core.exceptions import PermissionDenied
from functools import wraps

def is_admin(user):
    """
    Унифицированная проверка, является ли пользователь администратором
    
    Args:
        user: Экземпляр пользователя
        
    Returns:
        bool: True, если пользователь является администратором
    """
    # print('user.profile.user_type = ', user.profile.user_type)
    # print('user.is_superuser = ', user.is_superuser)
    # if user.is_superuser:
    #     return True
    
    if hasattr(user, "profile"):
        return user.profile.user_type == 0
    
    return False

def is_user1(user):
    """
    Унифицированная проверка, является ли пользователь пользователем 1 уровня
    
    Args:
        user: Экземпляр пользователя
        
    Returns:
        bool: True, если пользователь является пользователем 1 уровня
    """
    if user.is_superuser:
        return True
    
    if hasattr(user, "profile"):
        return user.profile.user_type == 1
    
    return False

def is_user2(user):
    """
    Унифицированная проверка, является ли пользователь пользователем 2 уровня
    
    Args:
        user: Экземпляр пользователя
        
    Returns:
        bool: True, если пользователь является пользователем 2 уровня
    """
    if hasattr(user, "profile"):
        return user.profile.user_type == 2
    
    return False

def can_view_logs(user):
    """
    Унифицированная проверка, может ли пользователь просматривать логи
    
    Args:
        user: Экземпляр пользователя
        
    Returns:
        bool: True, если пользователь может просматривать логи
    """
    return is_admin(user)

def can_import_data(user):
    """
    Проверяет, может ли пользователь импортировать данные
    
    Args:
        user: Экземпляр пользователя
        
    Returns:
        bool: True, если пользователь может импортировать данные
    """
    return is_admin(user)

def can_export_data(user):
    """
    Проверяет, может ли пользователь экспортировать данные
    
    Args:
        user: Экземпляр пользователя
        
    Returns:
        bool: True, если пользователь может экспортировать данные
    """
    if user.is_superuser:
        return True
    
    if hasattr(user, "profile"):
        return user.profile.can_export_data
    
    return False

def can_view_history(user):
    """
    Проверяет, может ли пользователь просматривать историю импорта
    
    Args:
        user: Экземпляр пользователя
        
    Returns:
        bool: True, если пользователь может просматривать историю импорта
    """
    return is_admin(user)

def can_view_imsi(user):
    """
    Проверяет, может ли пользователь видеть поле IMSI
    
    Args:
        user: Экземпляр пользователя
        
    Returns:
        bool: True, если пользователь может видеть поле IMSI
    """
    # Сначала проверяем user_type, даже для суперпользователей
    if hasattr(user, "profile"):
        return user.profile.user_type != 2
    
    # Если нет профиля, то только суперпользователи могут видеть IMSI
    return user.is_superuser

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

# Декораторы
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

def clean_password(password):
    """
    Проверяет и очищает пароль от крайних непечатных символов
    
    Args:
        password: Строка пароля
        
    Returns:
        tuple: (is_empty, cleaned_password)
            - is_empty: True если пароль пустой или содержит только непечатные символы
            - cleaned_password: Очищенный пароль без крайних непечатных символов
    """
    if not password:
        return True, ""
    
    # Убираем крайние пробелы и непечатные символы
    cleaned = password.strip()
    
    # Проверяем, содержит ли пароль только непечатные символы
    if not cleaned or cleaned.isspace():
        return True, ""
    
    return False, cleaned