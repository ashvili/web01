from django.urls import path
from django.contrib.auth.views import LoginView, LogoutView, PasswordResetView, PasswordResetDoneView, PasswordResetConfirmView, PasswordResetCompleteView

from . import views

app_name = 'accounts'

urlpatterns = [
    # Профиль пользователя
    path('profile/', views.ProfileView.as_view(), name='profile'),
    
    # Установка темы
    path('set-theme/', views.set_theme, name='set_theme'),
    
    # Страница OTP required
    path('otp-required/', views.OtpRequiredView.as_view(), name='otp_required'),
    
    # Стандартные маршруты авторизации Django
    path('login/', LoginView.as_view(template_name='accounts/login.html'), name='login'),
    path('logout/', LogoutView.as_view(), name='logout'),
    path('password-reset/', PasswordResetView.as_view(template_name='accounts/password_reset.html'), name='password_reset'),
    path('password-reset/done/', PasswordResetDoneView.as_view(template_name='accounts/password_reset_done.html'), name='password_reset_done'),
    path('password-reset/<uidb64>/<token>/', PasswordResetConfirmView.as_view(template_name='accounts/password_reset_confirm.html'), name='password_reset_confirm'),
    path('password-reset/complete/', PasswordResetCompleteView.as_view(template_name='accounts/password_reset_complete.html'), name='password_reset_complete'),
] 