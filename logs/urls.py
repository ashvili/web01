from django.urls import path
from . import views

app_name = 'logs'

urlpatterns = [
    path('', views.log_list, name='list'),
    path('<int:log_id>/', views.log_detail, name='detail'),
    path('export/', views.export_logs, name='export'),
    # Альтернативное представление на основе класса
    path('view/<int:log_id>/', views.LogDetailView.as_view(), name='view'),
    path('clear-old-logs/', views.clear_old_logs, name='clear_old_logs'),
] 