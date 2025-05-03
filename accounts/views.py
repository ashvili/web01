from django.shortcuts import render, redirect, get_object_or_404
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
from django.contrib.auth.decorators import login_required, user_passes_test
from django.views.decorators.csrf import csrf_protect
from django.conf import settings
from django_otp.plugins.otp_totp.models import TOTPDevice
from django.contrib.auth.models import User
from django.contrib.auth.hashers import make_password
from django.core.paginator import Paginator

from .forms import UserProfileForm
from .models import UserProfile

# Главная страница
class HomeView(TemplateView):
    template_name = 'accounts/home.html'

# Представление для профиля пользователя
class ProfileView(LoginRequiredMixin, UpdateView):
    template_name = 'accounts/profile.html'
    form_class = UserProfileForm
    success_url = reverse_lazy('accounts:profile')
    
    def get_object(self):
        return self.request.user.profile
    
    def form_valid(self, form):
        messages.success(self.request, 'Профиль успешно обновлен')
        return super().form_valid(form)

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
                if user.profile.totp_enabled:
                    return redirect('accounts:otp_required')
                return redirect('subscribers:search')
        
        return render(request, self.template_name, {'form': form})

def is_admin(user):
    return user.profile.role == 'admin'

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
        profile_form = UserProfileForm(request.POST)
        if user_form.is_valid() and profile_form.is_valid():
            user = user_form.save(commit=False)
            user.password = make_password(user_form.cleaned_data['password'])
            user.save()
            profile = profile_form.save(commit=False)
            profile.user = user
            profile.save()
            messages.success(request, 'Пользователь успешно создан')
            return redirect('user_list')
    else:
        user_form = UserForm()
        profile_form = UserProfileForm()
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
        profile_form = UserProfileForm(request.POST, instance=user.profile)
        if user_form.is_valid() and profile_form.is_valid():
            if user_form.cleaned_data.get('password'):
                user.password = make_password(user_form.cleaned_data['password'])
            user_form.save()
            profile_form.save()
            messages.success(request, 'Пользователь успешно обновлен')
            return redirect('user_list')
    else:
        user_form = UserForm(instance=user)
        profile_form = UserProfileForm(instance=user.profile)
    return render(request, 'accounts/user_form.html', {
        'user_form': user_form,
        'profile_form': profile_form
    })

@login_required
@user_passes_test(is_admin)
def user_delete(request, pk):
    user = get_object_or_404(User, pk=pk)
    if request.method == 'POST':
        user.delete()
        messages.success(request, 'Пользователь успешно удален')
        return redirect('user_list')
    return render(request, 'accounts/user_confirm_delete.html', {'user': user})
