from django.urls import path
from . import views

app_name = 'subscribers'

urlpatterns = [
    # Список абонентов
    path('', views.subscriber_list, name='list'),
    
    # Импорт данных из CSV
    path('import/', views.import_csv, name='import_csv'),
    
    # История импорта
    path('import/history/', views.import_history, name='import_history'),
    
    # Детали импорта
    path('import/history/<int:import_id>/', views.import_detail, name='import_detail'),
    path('import/cleanup-archives/', views.cleanup_archives, name='cleanup_archives'),
] 