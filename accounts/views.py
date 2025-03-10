from django.shortcuts import render, redirect
from django.views import View
from django.views.generic import TemplateView, FormView, UpdateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.forms import PasswordChangeForm
from django.contrib.auth import update_session_auth_hash
from django.urls import reverse_lazy
from django.contrib import messages
from django.http import JsonResponse, HttpResponseRedirect
from django.views.decorators.http import require_POST
from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_protect

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
class OtpRequiredView(TemplateView):
    template_name = 'accounts/otp_required.html'
