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
                return response
            # Логируем только если это не GET или если GET с параметрами
            if request.method == 'GET' and not request.GET:
                return response  # Не логируем просто открытие страницы
            duration_ms = (time.time() - getattr(request, '_start_time', time.time())) * 1000
            try:
                data = json.dumps(request.GET.dict() or request.POST.dict())
            except Exception:
                data = ''
                
            # Импортируем модель внутри метода для избежания циклических зависимостей
            from .models import UserActionLog
            
            UserActionLog.objects.create(
                user=request.user,
                action_type='OTHER',
                path=request.path,
                method=request.method,
                status_code=response.status_code,
                duration_ms=duration_ms,
                ip_address=get_client_ip(request),
                user_agent=request.META.get('HTTP_USER_AGENT', ''),
                additional_data=data,
            )
        return response 