import time
import json
from django.utils.deprecation import MiddlewareMixin

def get_client_ip(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        return x_forwarded_for.split(',')[0].strip()
    return request.META.get('REMOTE_ADDR')

class UserActionLoggingMiddleware(MiddlewareMixin):
    def process_request(self, request):
        request._start_time = time.time()

    def process_response(self, request, response):
        if request.user.is_authenticated:
            # Не логируем просто открытие страницы поиска абонентов (GET и POST)
            if request.path == '/subscribers/search/':
                # Но если есть параметры поиска — логируем как SEARCH
                if request.GET or request.POST:
                    action_type = 'SEARCH'
                else:
                    return response
            else:
                action_type = 'OTHER'
            if request.method == 'GET' and not request.GET:
                return response  # Не логируем просто открытие страницы
            duration_ms = (time.time() - getattr(request, '_start_time', time.time())) * 1000
            # Сохраняем параметры запроса как dict
            if request.method == 'POST':
                data = request.POST.dict()
            else:
                data = request.GET.dict()
            # Импортируем модель внутри метода для избежания циклических зависимостей
            from .models import UserActionLog, ContentType
            content_type = None
            object_id = None
            # Пробуем определить объект (например, если есть pk/id в url/query)
            if 'object_id' in data:
                try:
                    object_id = int(data['object_id'])
                except Exception:
                    pass
            elif 'id' in data:
                try:
                    object_id = int(data['id'])
                except Exception:
                    pass
            # Можно доработать: определять content_type по url/view
            UserActionLog.objects.create(
                user=request.user,
                action_type=action_type,
                path=request.path,
                method=request.method,
                status_code=response.status_code,
                duration_ms=duration_ms,
                ip_address=get_client_ip(request),
                user_agent=request.META.get('HTTP_USER_AGENT', ''),
                additional_data=data or None,
                content_type=content_type,
                object_id=object_id,
            )
        return response 