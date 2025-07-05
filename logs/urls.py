from django.urls import path
from . import views

app_name = 'logs'

urlpatterns = [
    path('', views.log_list, name='list'),
    path('<int:log_id>/', views.log_detail, name='detail'),
    path('export/', views.export_logs, name='export'),
    path('clear-old-logs/', views.clear_old_logs, name='clear_old_logs'),
    path('sessions/', views.log_sessions, name='sessions'),
    path('chain/<int:log_id>/', views.log_chain, name='chain'),
    path('activity/', views.activity_overview, name='activity'),
] 