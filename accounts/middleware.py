from django.shortcuts import redirect
from django.urls import reverse
import re

class TOTPMiddleware:
    """
    Middleware проверяет, настроена ли у пользователя 2FA,
    и если да, то перенаправляет на страницу ввода кода TOTP
    после стандартной аутентификации.
    """
    
    def __init__(self, get_response):
        self.get_response = get_response
        # Пути, для которых не нужна 2FA проверка
        self.exempt_urls = [
            r'^/accounts/otp/$',  # Страница ввода кода TOTP
            r'^/accounts/login/$',  # Страница входа
            r'^/accounts/logout/$',  # Страница выхода
            r'^/admin/',  # Административный раздел
            r'^/static/',  # Статические файлы
        ]
    
    def __call__(self, request):
        # Если пользователь аутентифицирован
        if request.user.is_authenticated:
            # Проверка, включена ли 2FA у пользователя
            try:
                if request.user.profile.totp_enabled:
                    # Проверка, прошел ли пользователь 2FA
                    if not request.session.get('otp_verified', False):
                        # Проверяем, не находимся ли мы уже на странице ввода 2FA
                        path = request.path
                        if not any(re.match(url, path) for url in self.exempt_urls):
                            # Перенаправляем на страницу ввода кода
                            return redirect('accounts:otp_required')
            except (AttributeError, Exception):
                # Если возникла ошибка, продолжаем без 2FA
                pass
        
        response = self.get_response(request)
        return response 