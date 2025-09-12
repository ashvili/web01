"""
URL configuration for vl09_web project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.conf.urls.i18n import i18n_patterns
from two_factor.urls import urlpatterns as tf_urls

from accounts.views import HomeView

urlpatterns = [
    path('admin/', admin.site.urls),
    path('i18n/', include('django.conf.urls.i18n')),  # URL для переключения языков
] + i18n_patterns(
    path('', HomeView.as_view(), name='home'),
    path('accounts/', include('accounts.urls')),
    path('subscribers/', include('subscribers.urls')),
    path('logs/', include('logs.urls')),
    path('2fa/', include(tf_urls, namespace='two_factor')),  # Изменили префикс на /2fa/ чтобы избежать конфликтов
)

# Добавляем статические файлы в режиме разработки
if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
