from django.urls import path
from django.views.generic import TemplateView

app_name = 'subscribers'

urlpatterns = [
    # Временная заглушка для списка абонентов
    path('', TemplateView.as_view(template_name='subscribers/list.html'), name='list'),
    # Временная заглушка для импорта данных
    path('import/', TemplateView.as_view(template_name='subscribers/import.html'), name='import'),
] 