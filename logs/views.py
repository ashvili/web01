from django.shortcuts import render, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.core.paginator import Paginator
from django.http import HttpResponse
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import DetailView
import csv
import io
import json
from datetime import datetime

from .models import UserActionLog
from .forms import LogFilterForm


def is_admin_or_staff(user):
    """Проверка, является ли пользователь администратором или сотрудником"""
    return user.is_staff or user.is_superuser or getattr(user.profile, 'user_type', None) == 0


@login_required
@user_passes_test(is_admin_or_staff)
def log_list(request):
    """Представление для просмотра списка логов действий пользователей"""
    form = LogFilterForm(request.GET or None)
    logs = UserActionLog.objects.all().order_by('-action_time')
    
    # Применяем фильтры, если форма валидна
    if request.GET and form.is_valid():
        logs = form.get_queryset()
    
    # Пагинация
    paginator = Paginator(logs, 25)  # 25 записей на страницу
    page = request.GET.get('page')
    logs_page = paginator.get_page(page)
    
    context = {
        'form': form,
        'logs': logs_page,
        'title': 'Журнал действий пользователей',
    }
    
    return render(request, 'logs/list.html', context)


@login_required
@user_passes_test(is_admin_or_staff)
def log_detail(request, log_id):
    """Представление для просмотра детальной информации о логе"""
    log = get_object_or_404(UserActionLog, id=log_id)
    
    # Форматируем additional_data как JSON если есть
    formatted_data = None
    if log.additional_data:
        try:
            formatted_data = json.dumps(log.additional_data, indent=2, ensure_ascii=False)
        except:
            formatted_data = str(log.additional_data)
    
    context = {
        'log': log,
        'title': f'Лог #{log.id}: {log.get_action_type_display()}',
        'formatted_data': formatted_data,
        # Добавляем классы для действий
        'action_classes': {
            'LOGIN': 'bg-success',
            'LOGOUT': 'bg-info',
            'CREATE': 'bg-primary',
            'UPDATE': 'bg-warning',
            'DELETE': 'bg-danger',
            'IMPORT': 'bg-primary',
            'EXPORT': 'bg-info',
            'SEARCH': 'bg-secondary',
            'OTHER': 'bg-secondary',
        }
    }
    
    return render(request, 'logs/detail.html', context)


@login_required
@user_passes_test(is_admin_or_staff)
def export_logs(request):
    """Представление для экспорта логов в CSV"""
    form = LogFilterForm(request.GET or None)
    
    # Получаем логи с применением фильтров
    if form.is_valid():
        logs = form.get_queryset()
    else:
        logs = UserActionLog.objects.all().order_by('-action_time')
    
    # Создаем CSV-файл в памяти
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    
    # Записываем заголовки
    writer.writerow([
        'ID', 'Пользователь', 'Тип действия', 'Время действия', 'IP адрес', 
        'User Agent', 'Путь запроса', 'HTTP метод', 'HTTP код', 'Длительность (мс)',
        'Тип объекта', 'ID объекта', 'Дополнительные данные'
    ])
    
    # Записываем данные
    for log in logs:
        writer.writerow([
            log.id,
            log.user.username,
            log.get_action_type_display(),
            log.action_time.strftime('%d.%m.%Y %H:%M:%S'),
            log.ip_address or '',
            log.user_agent or '',
            log.path or '',
            log.method or '',
            log.status_code or '',
            log.duration_ms or '',
            log.content_type.model if log.content_type else '',
            log.object_id or '',
            str(log.additional_data or '')
        ])
    
    # Создаем HTTP-ответ с CSV-файлом
    response = HttpResponse(buffer.getvalue(), content_type='text/csv')
    response['Content-Disposition'] = f'attachment; filename="logs_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv"'
    
    return response


class LogDetailView(LoginRequiredMixin, DetailView):
    """Представление-класс для просмотра детальной информации о логе"""
    model = UserActionLog
    template_name = 'logs/detail.html'
    context_object_name = 'log'
    pk_url_kwarg = 'log_id'
    
    def get_queryset(self):
        # Доступ только для администраторов и сотрудников
        if self.request.user.is_staff or self.request.user.is_superuser or getattr(self.request.user.profile, 'user_type', None) == 0:
            return super().get_queryset()
        return UserActionLog.objects.none()
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['title'] = f'Лог #{self.object.id}: {self.object.get_action_type_display()}'
        return context
