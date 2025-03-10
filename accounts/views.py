from django.shortcuts import render, redirect
from django.views import View
from django.views.generic import TemplateView, FormView, UpdateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.forms import PasswordChangeForm, AuthenticationForm
from django.contrib.auth import update_session_auth_hash, login, authenticate
from django.urls import reverse_lazy
from django.contrib import messages
from django.http import JsonResponse, HttpResponseRedirect
from django.views.decorators.http import require_POST
from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_protect
from django.conf import settings
from django_otp.plugins.otp_totp.models import TOTPDevice

from .forms import UserProfileForm
from .models import UserProfile

# Главная страница
class HomeView(TemplateView):
    template_name = 'home.html'

# Представление для профиля пользователя
class ProfileView(LoginRequiredMixin, View):
    template_name = 'accounts/profile.html'
    
    def get(self, request):
        profile_form = UserProfileForm(instance=request.user.profile)
        password_form = PasswordChangeForm(user=request.user)
        
        context = {
            'profile_form': profile_form,
            'password_form': password_form,
        }
        return render(request, self.template_name, context)
    
    @method_decorator(csrf_protect)
    def post(self, request):
        if 'profile_submit' in request.POST:
            profile_form = UserProfileForm(request.POST, instance=request.user.profile)
            if profile_form.is_valid():
                profile_form.save()
                messages.success(request, 'Профиль успешно обновлен.')
                return redirect('accounts:profile')
            
            password_form = PasswordChangeForm(user=request.user)
        
        elif 'password_submit' in request.POST:
            password_form = PasswordChangeForm(user=request.user, data=request.POST)
            if password_form.is_valid():
                user = password_form.save()
                update_session_auth_hash(request, user)
                messages.success(request, 'Пароль успешно изменен.')
                return redirect('accounts:profile')
            
            profile_form = UserProfileForm(instance=request.user.profile)
        
        context = {
            'profile_form': profile_form,
            'password_form': password_form,
        }
        return render(request, self.template_name, context)

# Представление для установки темы оформления
@require_POST
@login_required
def set_theme(request):
    theme = request.POST.get('theme', 'light')
    if theme in ['light', 'dark']:
        profile = request.user.profile
        profile.theme = theme
        profile.save()
    
    if request.headers.get('x-requested-with') == 'XMLHttpRequest':
        return JsonResponse({'status': 'success'})
    return HttpResponseRedirect(request.META.get('HTTP_REFERER', '/'))

# Страница с ошибкой доступа для двухфакторной аутентификации
class OtpRequiredView(View):
    template_name = 'accounts/otp_required.html'
    
    def get(self, request):
        # Проверяем, что у нас есть информация о пользователе в сессии
        if 'user_id' not in request.session or 'totp_required' not in request.session:
            return redirect('accounts:login')
            
        return render(request, self.template_name)
    
    def post(self, request):
        # Получаем введенный код
        token = request.POST.get('token')
        user_id = request.session.get('user_id')
        
        if not token or not user_id:
            messages.error(request, 'Необходимо ввести код из приложения аутентификации')
            return redirect('accounts:otp_required')
        
        from django.contrib.auth.models import User
        import pyotp
        
        try:
            user = User.objects.get(id=user_id)
            device = TOTPDevice.objects.get(user=user, confirmed=True)
            
            # Проверяем токен с использованием профиля пользователя, где хранится ключ
            user_profile = user.profile
            totp_secret = user_profile.totp_secret
            
            if totp_secret:
                # Используем напрямую ключ из профиля
                totp = pyotp.TOTP(totp_secret)
                is_valid = totp.verify(token, valid_window=4)
                
                if is_valid:
                    # Если токен верный, выполняем вход и очищаем сессию
                    from django.contrib.auth import login
                    login(request, user)
                    
                    if 'user_id' in request.session:
                        del request.session['user_id']
                    if 'totp_required' in request.session:
                        del request.session['totp_required']
                    
                    return redirect('accounts:profile')
                else:
                    messages.error(request, 'Неверный код. Попробуйте еще раз.')
            else:
                messages.error(request, 'Секретный ключ не найден. Обратитесь к администратору.')
        except Exception as e:
            messages.error(request, 'Произошла ошибка при проверке кода.')
            print(f"Ошибка при проверке OTP: {str(e)}")
            
        return redirect('accounts:otp_required')

class CustomTOTPSetupView(LoginRequiredMixin, View):
    """Представление для настройки TOTP с пользовательским именем."""
    
    def get(self, request):
        # Импортируем необходимые библиотеки
        import pyotp
        import qrcode
        import io
        import base64
        
        # Получаем профиль пользователя
        user_profile = request.user.profile
        
        # Генерируем или используем существующий ключ для TOTP
        if not user_profile.totp_secret:
            # Если ключа нет - генерируем новый
            secret_key = pyotp.random_base32()
            
            # Сохраняем ключ в профиле пользователя
            user_profile.totp_secret = secret_key
            user_profile.save()
        else:
            # Используем существующий ключ
            secret_key = user_profile.totp_secret
        
        # Генерируем URL для QR-кода с более понятным именем
        phone_number = user_profile.phone_number or "unknown"
        username = phone_number if phone_number and phone_number != "unknown" else request.user.username
        account_name = f"{username}@subscriber-management"
        issuer_name = "SubscriberManagement"  # Должно быть коротким и без специальных символов
        
        totp = pyotp.TOTP(secret_key)
        provisioning_uri = totp.provisioning_uri(name=account_name, issuer_name=issuer_name)
        
        # Генерируем QR-код
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )
        qr.add_data(provisioning_uri)
        qr.make(fit=True)
        
        img = qr.make_image(fill_color="black", back_color="white")
        
        # Конвертируем изображение в base64
        buffered = io.BytesIO()
        img.save(buffered, format="PNG")
        img_str = base64.b64encode(buffered.getvalue()).decode('utf-8')
        qr_url = f"data:image/png;base64,{img_str}"
        
        context = {
            'qr_url': qr_url,
            'secret_key': secret_key,
        }
        
        return render(request, 'accounts/totp_setup.html', context)
    
    def post(self, request):
        import pyotp
        import logging
        
        token = request.POST.get('token')
        
        try:
            # Получаем секретный ключ из профиля пользователя
            user_profile = request.user.profile
            secret_key = user_profile.totp_secret
            
            # Отладочная информация только в журнале
            logger = logging.getLogger(__name__)
            logger.info(f"Проверка токена для пользователя {request.user.username}")
            
            # Дополнительная проверка и очистка токена
            if token and secret_key:
                token = token.strip()  # Удаляем лишние пробелы
                
                # Проверяем, что токен состоит только из цифр
                if token.isdigit():
                    # Использовать pyotp для проверки
                    totp = pyotp.TOTP(secret_key)
                    
                    # Проверяем токен с более широким окном (до 2 минут в прошлое и будущее)
                    is_valid = totp.verify(token, valid_window=4)  # 4 * 30 секунд = 2 минуты
                    
                    if is_valid:  # Проверяем с окном в 2 минуты
                        # Активируем 2FA для пользователя
                        user_profile.totp_enabled = True
                        user_profile.save()
                        
                        # Также создаем/обновляем запись в TOTPDevice для совместимости с django-otp
                        try:
                            device = TOTPDevice.objects.get(user=request.user)
                        except TOTPDevice.DoesNotExist:
                            device = TOTPDevice(user=request.user, name=user_profile.phone_number or request.user.username)
                            
                        device.confirmed = True
                        device.save()
                        
                        messages.success(request, 'Двухфакторная аутентификация успешно настроена!')
                        return redirect('accounts:profile')
                    else:
                        logger.warning(f"Неверный токен для пользователя {request.user.username}")
                        
                        # Даем более подробную информацию пользователю
                        messages.error(request, 'Неверный код. Возможные причины: несинхронизированное время на устройстве или неправильно введенный код. Попробуйте ввести новый код из приложения.')
                else:
                    messages.error(request, 'Код должен содержать только цифры.')
            else:
                if not secret_key:
                    messages.error(request, 'Секретный ключ не найден. Пожалуйста, начните настройку заново.')
                elif not token:
                    messages.error(request, 'Введите код из приложения для подтверждения.')
            
            # Возвращаемся на страницу настройки 2FA
            return redirect('accounts:totp_setup')
            
        except Exception as e:
            messages.error(request, 'Произошла ошибка при проверке кода. Пожалуйста, попробуйте еще раз.')
            logging.getLogger(__name__).error(f"Ошибка TOTP: {str(e)}", exc_info=True)
            
            return redirect('accounts:totp_setup')

class DisableTOTPView(LoginRequiredMixin, View):
    """Представление для отключения двухфакторной аутентификации."""
    
    def post(self, request):
        # Отключаем 2FA в профиле пользователя
        user_profile = request.user.profile
        user_profile.totp_secret = None
        user_profile.totp_enabled = False
        user_profile.save()
        
        # Также удаляем все TOTP-устройства пользователя для совместимости с django-otp
        devices = TOTPDevice.objects.filter(user=request.user)
        if devices.exists():
            devices.delete()
        
        messages.success(request, 'Двухфакторная аутентификация отключена.')
        return redirect('accounts:profile')

# Представление для входа с поддержкой 2FA
class LoginView(FormView):
    template_name = 'accounts/login.html'
    form_class = AuthenticationForm
    success_url = '/'

    def form_valid(self, form):
        username = form.cleaned_data.get('username')
        password = form.cleaned_data.get('password')
        user = authenticate(username=username, password=password)

        if user is not None:
            # Проверяем, есть ли у пользователя активное TOTP-устройство
            has_totp = TOTPDevice.objects.filter(user=user, confirmed=True).exists()

            if has_totp:
                # Если у пользователя есть TOTP, сохраняем информацию о пользователе в сессии
                # и перенаправляем на страницу ввода TOTP
                self.request.session['user_id'] = user.id
                self.request.session['totp_required'] = True
                return redirect('accounts:otp_required')
            else:
                # Если нет TOTP, просто выполняем вход
                login(self.request, user)
                return redirect(self.get_success_url())
        return super().form_valid(form)
