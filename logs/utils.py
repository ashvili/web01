import time
import functools
import inspect
from django.utils.decorators import method_decorator
import uuid
from datetime import timedelta
from django.conf import settings
from django.db import transaction
from .models import UserActionLog
from django.utils import timezone

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


def assign_logical_sessions(user, gap_hours=5):
    """
    Группирует логи пользователя по логическим сессиям (gap-based) и присваивает logical_session_id.
    Новая сессия начинается, если между действиями прошло больше gap_hours.
    gap_hours можно переопределить через settings.LOGICAL_SESSION_GAP_HOURS.
    """
    gap = getattr(settings, 'LOGICAL_SESSION_GAP_HOURS', gap_hours)
    logs = UserActionLog.objects.filter(user=user).order_by('action_time')
    last_time = None
    current_session_id = None
    with transaction.atomic():
        for log in logs:
            if last_time is None or (log.action_time - last_time).total_seconds() >= gap * 3600:
                current_session_id = uuid.uuid4()
            log.logical_session_id = current_session_id
            log.save(update_fields=['logical_session_id'])
            last_time = log.action_time 

def test_gap_based_assignment(self):
    now = timezone.now()
    # 3 действия: 0ч, +2ч, +8ч (gap=5)
    log1 = UserActionLog.objects.create(user=self.user, action_type='LOGIN', action_time=now)
    log2 = UserActionLog.objects.create(user=self.user, action_type='SEARCH', action_time=now + timedelta(hours=2))
    log3 = UserActionLog.objects.create(user=self.user, action_type='LOGOUT', action_time=now + timedelta(hours=8))
    assign_logical_sessions(self.user, gap_hours=5)
    log1.refresh_from_db()
    log2.refresh_from_db()
    log3.refresh_from_db()
    print('log1:', log1.action_time, log1.logical_session_id)
    print('log2:', log2.action_time, log2.logical_session_id)
    print('log3:', log3.action_time, log3.logical_session_id)
    self.assertEqual(log1.logical_session_id, log2.logical_session_id)
    self.assertNotEqual(log1.logical_session_id, log3.logical_session_id) 

def log_related_action(request, user, action_type, related_log=None, obj=None, additional_data=None):
    """
    Логирует действие пользователя с указанием related_log (цепочка событий).
    """
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
        related_log=related_log,
    ) 