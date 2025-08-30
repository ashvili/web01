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
    path('import/status/<int:import_id>/', views.import_status, name='import_status'),
    path('import/resume/<int:import_id>/', views.import_resume, name='import_resume'),
    path('import/pause/<int:import_id>/', views.import_pause, name='import_pause'),
    path('import/cancel/<int:import_id>/', views.import_cancel, name='import_cancel'),
    path('import/finalize/<int:import_id>/', views.import_finalize, name='import_finalize'),
    path('import/errors/<int:import_id>/', views.import_errors, name='import_errors'),
    path('import/cleanup-archives/', views.cleanup_archives, name='cleanup_archives'),
    path('import/list-archives/', views.list_archives, name='list_archives'),
    
    # Поиск абонентов
    path('search/', views.search_subscribers, name='search'),
    
    # Детали абонента
    path('subscriber/<int:subscriber_id>/', views.subscriber_detail, name='subscriber_detail'),
] 