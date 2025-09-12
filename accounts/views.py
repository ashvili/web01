from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.views.generic import TemplateView, FormView, UpdateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.forms import AuthenticationForm
from django.contrib.auth import update_session_auth_hash, login, authenticate, logout
from django.urls import reverse_lazy
from django.contrib import messages
from django.http import JsonResponse, HttpResponseRedirect
from django.views.decorators.http import require_POST
from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required, user_passes_test
from django.views.decorators.csrf import csrf_protect
from django.conf import settings
from django_otp.plugins.otp_totp.models import TOTPDevice
from django.contrib.auth.models import User
from django.contrib.auth.hashers import make_password
from django.core.paginator import Paginator
import pyotp

from .forms import UserProfileForm, UserForm, TOTPForm, PasswordChangeForm
from .models import UserProfile

# Импортируем API для логирования из utils напрямую
from logs.utils import (
    log_login, 
    log_logout, 
    log_action_decorator, 
    LogUserAction
)

# Главная страница (администратор) или перенаправление на поиск абонентов для остальных
class HomeView(LoginRequiredMixin, TemplateView):
    template_name = 'accounts/home.html'

    def get(self, request, *args, **kwargs):
        # Перенаправляем на поиск абонентов
        return redirect('subscribers:search')
        # Если пользователь не администратор, перенаправляем на поиск абонентов
        # if not request.user.profile.is_admin():
        #     return redirect('subscribers:search')
        # return super().get(request, *args, **kwargs)

# Представление для профиля пользователя
class ProfileView(LoginRequiredMixin, View):
    template_name = 'accounts/profile.html'
    
    def get(self, request):
        # Создаем initial данные для полей пользователя
        initial_data = {
            'first_name': request.user.first_name,
            'last_name': request.user.last_name,
            'email': request.user.email,
        }
        
        profile_form = UserProfileForm(instance=request.user.profile, user=request.user, initial=initial_data)
        password_form = PasswordChangeForm(request.user)
        
        context = {
            'profile_form': profile_form,
            'password_form': password_form,
        }
        return render(request, self.template_name, context)
    
    def post(self, request):
        # Создаем initial данные для полей пользователя
        initial_data = {
            'first_name': request.user.first_name,
            'last_name': request.user.last_name,
            'email': request.user.email,
        }
        
        if 'profile_submit' in request.POST:
            profile_form = UserProfileForm(request.POST, instance=request.user.profile, user=request.user, initial=initial_data)
            password_form = PasswordChangeForm(request.user)
            
            if profile_form.is_valid():
                profile_form.save()
                messages.success(request, 'Профиль успешно обновлен')
                return redirect('accounts:profile')
            else:
                # Если форма невалидна, показываем ошибки
                messages.error(request, 'Пожалуйста, исправьте ошибки в форме')
        elif 'password_submit' in request.POST:
            profile_form = UserProfileForm(instance=request.user.profile, user=request.user, initial=initial_data)
            password_form = PasswordChangeForm(request.user, request.POST)
            
            if password_form.is_valid():
                password_form.save()
                messages.success(request, 'Пароль успешно изменен')
                return redirect('accounts:profile')
            else:
                # Если форма невалидна, показываем ошибки
                messages.error(request, 'Пожалуйста, исправьте ошибки в форме пароля')
        else:
            profile_form = UserProfileForm(instance=request.user.profile, user=request.user, initial=initial_data)
            password_form = PasswordChangeForm(request.user)
        
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
class OtpRequiredView(LoginRequiredMixin, View):
    template_name = 'accounts/otp.html'
    
    def get(self, request):
        if not request.user.profile.totp_enabled:
            return redirect('subscribers:search')
        return render(request, self.template_name)
    
    def post(self, request):
        if not request.user.profile.totp_enabled:
            return redirect('subscribers:search')
        
        import pyotp
        
        token = request.POST.get('token')
        secret = request.user.profile.totp_secret
        
        if not secret:
            messages.error(request, '2FA не настроена')
            return redirect('subscribers:search')
        
        totp = pyotp.TOTP(secret)
        if totp.verify(token):
            request.session['otp_verified'] = True
            return redirect('subscribers:search')
        
        messages.error(request, 'Неверный код')
        return render(request, self.template_name)

class CustomTOTPSetupView(LoginRequiredMixin, View):
    template_name = 'accounts/totp_setup.html'
    
    def get(self, request):
        if request.user.profile.totp_enabled:
            return redirect('accounts:profile')
        
        import pyotp
        import qrcode
        import base64
        from io import BytesIO
        
        secret = pyotp.random_base32()
        totp = pyotp.TOTP(secret)
        provisioning_uri = totp.provisioning_uri(
            request.user.email,
            issuer_name=settings.OTP_TOTP_ISSUER
        )
        
        # Генерация QR-кода как PNG в base64
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )
        qr.add_data(provisioning_uri)
        qr.make(fit=True)
        
        img = qr.make_image(fill_color="black", back_color="white")
        buffer = BytesIO()
        img.save(buffer)
        buffer.seek(0)
        qr_code = base64.b64encode(buffer.getvalue()).decode()
        
        context = {
            'secret': secret,
            'qr_code': f"data:image/png;base64,{qr_code}"
        }
        return render(request, self.template_name, context)
    
    def post(self, request):
        if request.user.profile.totp_enabled:
            return redirect('accounts:profile')
        
        import pyotp
        
        secret = request.POST.get('secret')
        token = request.POST.get('token')
        
        if not secret or not token:
            messages.error(request, 'Неверные данные')
            return redirect('accounts:totp_setup')
        
        totp = pyotp.TOTP(secret)
        if totp.verify(token):
            request.user.profile.totp_secret = secret
            request.user.profile.totp_enabled = True
            request.user.profile.save()
            messages.success(request, '2FA успешно настроена')
            return redirect('accounts:profile')
        
        messages.error(request, 'Неверный код')
        return redirect('accounts:totp_setup')

class DisableTOTPView(LoginRequiredMixin, View):
    template_name = 'accounts/disable_totp.html'
    
    def get(self, request):
        if not request.user.profile.totp_enabled:
            return redirect('accounts:profile')
        return render(request, self.template_name)
    
    def post(self, request):
        if not request.user.profile.totp_enabled:
            return redirect('accounts:profile')
        
        import pyotp
        
        token = request.POST.get('token')
        secret = request.user.profile.totp_secret
        
        if not secret:
            messages.error(request, '2FA не настроена')
            return redirect('accounts:profile')
        
        totp = pyotp.TOTP(secret)
        if totp.verify(token):
            request.user.profile.totp_secret = None
            request.user.profile.totp_enabled = False
            request.user.profile.save()
            messages.success(request, '2FA отключена')
            return redirect('accounts:profile')
        
        messages.error(request, 'Неверный код')
        return render(request, self.template_name)

# Представление для входа с поддержкой 2FA
class LoginView(View):
    template_name = 'accounts/login.html'
    
    def get(self, request):
        if request.user.is_authenticated:
            return redirect('subscribers:search')
        form = AuthenticationForm()
        return render(request, self.template_name, {'form': form})
    
    def post(self, request):
        form = AuthenticationForm(data=request.POST)
        if form.is_valid():
            username = form.cleaned_data.get('username')
            password = form.cleaned_data.get('password')
            user = authenticate(username=username, password=password)
            if user is not None:
                login(request, user)
                from logs.utils import log_login  # импорт внутри метода, чтобы избежать циклических импортов
                log_login(request, user)
                if user.profile.totp_enabled:
                    return redirect('accounts:otp_required')
                return redirect('subscribers:search')
        
        return render(request, self.template_name, {'form': form})

def is_admin(user):
    return user.profile.is_admin()

@login_required
@user_passes_test(is_admin)
def user_list(request):
    users = User.objects.all().order_by('-date_joined')
    paginator = Paginator(users, 10)
    page = request.GET.get('page')
    users = paginator.get_page(page)
    return render(request, 'accounts/user_list.html', {'users': users})

@login_required
@user_passes_test(is_admin)
def user_create(request):
    if request.method == 'POST':
        user_form = UserForm(request.POST)
        profile_form = UserProfileForm(request.POST, user=None)
        if user_form.is_valid() and profile_form.is_valid():
            # Сохраняем пользователя
            user = user_form.save(commit=False)
            user.password = make_password(user_form.cleaned_data['password'])
            user.save()
            
            # Обновляем профиль, который был автоматически создан через сигнал
            profile = user.profile
            # Применяем значения из формы
            for field_name, field_value in profile_form.cleaned_data.items():
                setattr(profile, field_name, field_value)
            profile.save()
            
            messages.success(request, 'Пользователь успешно создан')
            return redirect('accounts:user_list')
    else:
        user_form = UserForm()
        profile_form = UserProfileForm(user=None)
    return render(request, 'accounts/user_form.html', {
        'user_form': user_form,
        'profile_form': profile_form
    })

@login_required
@user_passes_test(is_admin)
def user_edit(request, pk):
    user = get_object_or_404(User, pk=pk)
    if request.method == 'POST':
        user_form = UserForm(request.POST, instance=user)
        profile_form = UserProfileForm(request.POST, instance=user.profile, user=user)
        totp_form = TOTPForm(request.POST, user=user)
        
        if user_form.is_valid() and profile_form.is_valid() and totp_form.is_valid():
            # Обработка настроек 2FA
            totp_enabled = totp_form.cleaned_data.get('totp_enabled')
            reset_totp = totp_form.cleaned_data.get('reset_totp')
            
            # Сброс настроек 2FA, если запрошено
            if reset_totp:
                user.profile.totp_secret = None
                user.profile.totp_enabled = False
                user.profile.save()
                messages.success(request, '2FA отключена и сброшена')
            
            # Если 2FA была включена и она не была ранее настроена или была сброшена
            elif totp_enabled and (not user.profile.totp_enabled or reset_totp):
                # Генерируем новый секрет
                secret = pyotp.random_base32()
                user.profile.totp_secret = secret
                user.profile.totp_enabled = True
                user.profile.save()
                messages.success(request, 'Необходимо завершить настройку 2FA')
                # Перенаправляем на страницу завершения настройки 2FA для этого пользователя
                return redirect('accounts:admin_2fa_setup', pk=user.id)
            
            # Если 2FA была отключена
            elif not totp_enabled and user.profile.totp_enabled:
                user.profile.totp_enabled = False
                user.profile.save()
                messages.success(request, '2FA отключена')
            
            # Обработка данных пользователя
            # Пароль обрабатывается в форме UserForm.save()
            user_form.save()
            profile_form.save()
            
            messages.success(request, 'Пользователь успешно обновлен')
            return redirect('accounts:user_list')
    else:
        user_form = UserForm(instance=user)
        profile_form = UserProfileForm(instance=user.profile, user=user)
        totp_form = TOTPForm(user=user)
    
    return render(request, 'accounts/user_form.html', {
        'user_form': user_form,
        'profile_form': profile_form,
        'totp_form': totp_form
    })

@login_required
@user_passes_test(is_admin)
def user_delete(request, pk):
    user = get_object_or_404(User, pk=pk)
    if request.method == 'POST':
        user.delete()
        messages.success(request, 'Пользователь успешно удален')
        return redirect('accounts:user_list')
    return render(request, 'accounts/user_confirm_delete.html', {'user': user})

@login_required
@user_passes_test(is_admin)
def admin_2fa_setup(request, pk):
    target_user = get_object_or_404(User, pk=pk)
    
    if not target_user.profile.totp_enabled or not target_user.profile.totp_secret:
        messages.error(request, '2FA не включена для этого пользователя')
        return redirect('accounts:user_edit', pk=pk)
    
    if request.method == 'POST':
        token = request.POST.get('token')
        if not token:
            messages.error(request, 'Введите код подтверждения')
            return render(request, 'accounts/admin_2fa_setup.html', {'user': target_user})
        
        totp = pyotp.TOTP(target_user.profile.totp_secret)
        if totp.verify(token):
            messages.success(request, '2FA успешно настроена')
            return redirect('accounts:user_edit', pk=pk)
        else:
            messages.error(request, 'Неверный код')
    
    # Генерируем QR-код
    import qrcode
    import base64
    from io import BytesIO
    
    secret = target_user.profile.totp_secret
    totp = pyotp.TOTP(secret)
    provisioning_uri = totp.provisioning_uri(
        target_user.email or target_user.username,
        issuer_name=settings.OTP_TOTP_ISSUER
    )
    
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(provisioning_uri)
    qr.make(fit=True)
    
    img = qr.make_image(fill_color="black", back_color="white")
    buffer = BytesIO()
    img.save(buffer)
    buffer.seek(0)
    qr_code = base64.b64encode(buffer.getvalue()).decode()
    
    context = {
        'user': target_user,
        'secret': secret,
        'qr_code': f"data:image/png;base64,{qr_code}"
    }
    
    return render(request, 'accounts/admin_2fa_setup.html', context)

# Пример использования функции-помощника для логирования входа
def custom_login_view(request):
    # Логика обработки формы входа
    if request.method == 'POST':
        # ... код аутентификации пользователя ...
        user = User.objects.get(username=request.POST.get('username'))
        login(request, user)
        
        # Логируем вход пользователя
        log_login(request, user)
        
        return redirect('home')
    return render(request, 'accounts/login.html')

# Пример использования функции-помощника для логирования выхода
def custom_logout_view(request):
    if request.user.is_authenticated:
        # Логируем выход до того, как пользователь будет разлогинен
        log_logout(request, request.user)
    
    logout(request)
    return redirect('login')

# Пример использования декоратора для функций
@log_action_decorator('UPDATE')
@login_required
def profile_update(request):
    if request.method == 'POST':
        # ... обновление профиля ...
        return redirect('profile')
    return render(request, 'accounts/profile_form.html')

# Пример использования декоратора для класса
@method_decorator(LogUserAction('UPDATE'), name='post')
class ProfileUpdateView(LoginRequiredMixin, UpdateView):
    model = User
    template_name = 'accounts/profile_form.html'
    fields = ['first_name', 'last_name', 'email']
    
    def get_object(self):
        return self.request.user
