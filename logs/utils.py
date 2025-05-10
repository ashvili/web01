import time
import functools
import inspect
from django.utils.decorators import method_decorator

# Отложенный импорт ContentType и UserActionLog
# для предотвращения ошибки AppRegistryNotReady
def get_content_type():
    from django.contrib.contenttypes.models import ContentType
    return ContentType

def get_user_action_log_model():
    from .models import UserActionLog
    return UserActionLog

def get_client_ip(request):
    """
    Получает IP-адрес клиента из запроса
    """
    from .middleware import get_client_ip as middleware_get_client_ip
    return middleware_get_client_ip(request)

"""
API для программного логирования действий пользователей.

Для логирования действий можно использовать:

1. Middleware (автоматически логирует все запросы):
   - Уже настроен в settings.py
   - Логирует все авторизованные запросы как тип 'OTHER'

2. Декораторы для функций и методов:
   - @log_action_decorator('ACTION_TYPE')
   - @method_decorator(LogUserAction('ACTION_TYPE'), name='method_name')

3. Прямой вызов функций логирования в коде:
   - log_create(request, user, obj, additional_data)
   - log_update(request, user, obj, additional_data)
   - log_delete(request, user, obj, additional_data)
   - log_login(request, user)
   - log_logout(request, user)
   - log_import(request, user, additional_data)
   - log_export(request, user, additional_data)
   - log_search(request, user, additional_data)

Примеры использования:

# Логирование через декоратор функции:
@log_action_decorator('IMPORT')
@login_required
def import_data(request):
    # ... код функции ...
    return response

# Логирование через декоратор класса:
@method_decorator(LogUserAction('UPDATE'), name='post')
class SubscriberUpdateView(UpdateView):
    # ... код класса ...

# Логирование через прямой вызов:
def some_view(request):
    subscriber = Subscriber.objects.get(pk=1)
    # ... изменение данных ...
    log_update(request, request.user, subscriber, {'changed_fields': ['phone', 'email']})
    return response
"""


def log_user_action(action_type, obj=None, additional_data=None):
    """
    Функция-помощник для логирования действий пользователя.
    
    Параметры:
    - action_type: тип действия (из UserActionLog.ACTION_TYPES)
    - obj: объект, над которым выполняется действие (опционально)
    - additional_data: дополнительные данные в формате JSON (опционально)
    
    Пример использования:
    log_user_action('CREATE', subscriber, {'source': 'manual_input'})
    """
    def log_action(request, user, action_type, obj=None, additional_data=None):
        if not user or not user.is_authenticated:
            return None
            
        content_type = None
        object_id = None
        
        if obj:
            ContentType = get_content_type()
            content_type = ContentType.objects.get_for_model(obj)
            object_id = obj.pk
            
        UserActionLog = get_user_action_log_model()
        return UserActionLog.objects.create(
            user=user,
            action_type=action_type,
            ip_address=get_client_ip(request) if request else None,
            user_agent=request.META.get('HTTP_USER_AGENT', '') if request else None,
            content_type=content_type,
            object_id=object_id,
            additional_data=additional_data,
            path=request.path if request else '/',
            method=request.method if request else 'SCRIPT',
        )
    
    return log_action


def log_action_decorator(action_type):
    """
    Декоратор для логирования действий в функциях.
    
    Параметры:
    - action_type: тип действия (из UserActionLog.ACTION_TYPES)
    
    Пример использования:
    @log_action_decorator('IMPORT')
    def import_data(request, ...):
        ...
    """
    def decorator(view_func):
        @functools.wraps(view_func)
        def wrapper(request, *args, **kwargs):
            start_time = time.time()
            result = view_func(request, *args, **kwargs)
            duration_ms = (time.time() - start_time) * 1000
            
            # Пытаемся найти объект в результате или параметрах
            obj = None
            for param in list(kwargs.values()) + list(args):
                if hasattr(param, 'pk') and not callable(param):
                    obj = param
                    break
            
            # Собираем дополнительные данные из параметров
            safe_kwargs = {}
            for key, value in kwargs.items():
                if isinstance(value, (str, int, float, bool, list, dict)) or value is None:
                    safe_kwargs[key] = value
            
            UserActionLog = get_user_action_log_model()
            ContentType = get_content_type()
            
            UserActionLog.objects.create(
                user=request.user,
                action_type=action_type,
                ip_address=get_client_ip(request),
                user_agent=request.META.get('HTTP_USER_AGENT', ''),
                content_type=ContentType.objects.get_for_model(obj) if obj else None,
                object_id=obj.pk if obj else None,
                additional_data=safe_kwargs or None,
                path=request.path,
                method=request.method,
                duration_ms=duration_ms,
            )
            return result
        return wrapper
    return decorator


class LogUserAction:
    """
    Декоратор для класса-представления для логирования действий.
    
    Параметры:
    - action_type: тип действия (из UserActionLog.ACTION_TYPES)
    - get_object_method: имя метода для получения объекта (по умолчанию 'get_object')
    
    Пример использования:
    @method_decorator(LogUserAction('UPDATE'), name='post')
    class SubscriberUpdateView(UpdateView):
        ...
    """
    def __init__(self, action_type, get_object_method='get_object'):
        self.action_type = action_type
        self.get_object_method = get_object_method
        
    def __call__(self, view_func):
        @functools.wraps(view_func)
        def wrapped_view(view_instance, request, *args, **kwargs):
            start_time = time.time()
            result = view_func(view_instance, request, *args, **kwargs)
            duration_ms = (time.time() - start_time) * 1000
            
            # Пытаемся получить объект
            obj = None
            if hasattr(view_instance, self.get_object_method):
                try:
                    get_object = getattr(view_instance, self.get_object_method)
                    obj = get_object()
                except Exception:
                    pass
            
            # Собираем данные из запроса
            method = request.method
            if method in ('POST', 'PUT', 'PATCH'):
                data = request.POST.dict()
            else:
                data = request.GET.dict()
            
            # Удаляем чувствительные данные
            if 'password' in data:
                data['password'] = '******'
            if 'csrfmiddlewaretoken' in data:
                del data['csrfmiddlewaretoken']
            
            UserActionLog = get_user_action_log_model()
            ContentType = get_content_type()
                
            UserActionLog.objects.create(
                user=request.user,
                action_type=self.action_type,
                ip_address=get_client_ip(request),
                user_agent=request.META.get('HTTP_USER_AGENT', ''),
                content_type=ContentType.objects.get_for_model(obj) if obj else None,
                object_id=obj.pk if obj else None,
                additional_data=data or None,
                path=request.path,
                method=method,
                duration_ms=duration_ms,
            )
            return result
        return wrapped_view


# Предопределенные функции для типичных действий
def log_login(request, user):
    """Логирование входа пользователя в систему"""
    log_func = log_user_action('LOGIN')
    return log_func(request, user, 'LOGIN', additional_data={'success': True})


def log_logout(request, user):
    """Логирование выхода пользователя из системы"""
    log_func = log_user_action('LOGOUT')
    return log_func(request, user, 'LOGOUT')


def log_create(request, user, obj, additional_data=None):
    """Логирование создания объекта"""
    log_func = log_user_action('CREATE')
    return log_func(request, user, 'CREATE', obj, additional_data)


def log_update(request, user, obj, additional_data=None):
    """Логирование обновления объекта"""
    log_func = log_user_action('UPDATE')
    return log_func(request, user, 'UPDATE', obj, additional_data)


def log_delete(request, user, obj, additional_data=None):
    """Логирование удаления объекта"""
    log_func = log_user_action('DELETE')
    return log_func(request, user, 'DELETE', obj, additional_data)


def log_import(request, user, additional_data=None):
    """Логирование импорта данных"""
    log_func = log_user_action('IMPORT')
    return log_func(request, user, 'IMPORT', additional_data=additional_data)


def log_export(request, user, additional_data=None):
    """Логирование экспорта данных"""
    log_func = log_user_action('EXPORT')
    return log_func(request, user, 'EXPORT', additional_data=additional_data)


def log_search(request, user, additional_data=None):
    """Логирование поиска"""
    log_func = log_user_action('SEARCH')
    return log_func(request, user, 'SEARCH', additional_data=additional_data) 