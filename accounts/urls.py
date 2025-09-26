from django.urls import path
from django.contrib.auth import views as auth_views

from . import views

app_name = 'accounts'

urlpatterns = [
    path('', views.HomeView.as_view(), name='home'),
    path('login/', views.LoginView.as_view(), name='login'),
    # Используем кастомный logout, чтобы явно логировать тип LOGOUT
    path('logout/', views.custom_logout_view, name='logout'),
    path('profile/', views.ProfileView.as_view(), name='profile'),
    path('otp-required/', views.OtpRequiredView.as_view(), name='otp_required'),
    
    # Настройка TOTP
    path('totp-setup/', views.CustomTOTPSetupView.as_view(), name='totp_setup'),
    path('disable-totp/', views.DisableTOTPView.as_view(), name='disable_totp'),
    
    
    # Управление пользователями
    path('users/', views.user_list, name='user_list'),
    path('users/create/', views.user_create, name='user_create'),
    path('users/<int:pk>/edit/', views.user_edit, name='user_edit'),
    path('users/<int:pk>/delete/', views.user_delete, name='user_delete'),
    path('users/<int:pk>/2fa-setup/', views.admin_2fa_setup, name='admin_2fa_setup'),
] 