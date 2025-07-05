from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required, user_passes_test
from django.core.paginator import Paginator
from django.http import HttpResponse
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import DetailView
import csv
import io
import json
import datetime
from django.conf import settings
from django.contrib import messages

from .models import UserActionLog
from .forms import LogFilterForm


def is_superadmin(user):
    """Проверка, является ли пользователь только суперпользователем"""
    return user.is_superuser


@login_required
@user_passes_test(is_superadmin)
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

    # --- ДОБАВЛЕНО: формируем детали для каждого лога ---
    logs_with_details = []
    for log in logs_page:
        details = ''
        if log.action_type == 'SEARCH' and log.additional_data:
            query = log.additional_data.get('query', {})
            fields = []
            for key in ['address', 'passport', 'full_name', 'phone_number']:
                value = query.get(key)
                if value:
                    fields.append(str(value))
            details = '; '.join(fields)
        log.details = details
        logs_with_details.append(log)
    # --- КОНЕЦ ДОБАВЛЕНИЯ ---

    context = {
        'form': form,
        'logs': logs_with_details,  # заменили на logs_with_details
        'title': 'Журнал действий пользователей',
    }
    
    return render(request, 'logs/list.html', context)


@login_required
@user_passes_test(is_superadmin)
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
@user_passes_test(is_superadmin)
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
        # Доступ только для суперпользователя
        if self.request.user.is_superuser:
            return super().get_queryset()
        return UserActionLog.objects.none()
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['title'] = f'Лог #{self.object.id}: {self.object.get_action_type_display()}'
        return context


@login_required
@user_passes_test(is_superadmin)
def clear_old_logs(request):
    """Ручная очистка устаревших логов (по кнопке)"""
    if request.method == 'POST':
        days = getattr(settings, 'USER_ACTION_LOG_RETENTION_DAYS', 90)
        cutoff = datetime.datetime.now() - datetime.timedelta(days=days)
        deleted, _ = UserActionLog.objects.filter(action_time__lt=cutoff).delete()
        messages.success(request, f'Удалено {deleted} логов старше {days} дней.')
    else:
        messages.error(request, 'Некорректный метод запроса.')
    return redirect('logs:list')
