from django.urls import path
from django.views.generic import TemplateView

app_name = 'logs'

urlpatterns = [
    # Временная заглушка для списка логов
    path('', TemplateView.as_view(template_name='logs/list.html'), name='list'),
] 