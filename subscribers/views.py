from django.shortcuts import render
import csv
import io
import datetime
import logging
import base64
from django.shortcuts import redirect, get_object_or_404
from django.contrib import messages
from django.contrib.auth.decorators import login_required, user_passes_test
from django.core.paginator import Paginator
from django.db import transaction
from django.http import HttpResponse
from django.utils import timezone
from django.urls import reverse
from django.db import models
from django.views.generic import ListView, DetailView, CreateView, UpdateView, DeleteView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.utils.decorators import method_decorator
from django.urls import reverse_lazy

from .models import Subscriber, ImportHistory
from .forms import CSVImportForm, ImportCSVForm, SearchForm
from .tasks import process_csv_import_task_impl

# Настройка логирования
logger = logging.getLogger(__name__)

# Импортируем API для логирования из utils напрямую
from logs.utils import (
    log_create,
    log_update,
    log_delete,
    log_search,
    log_import,
    log_export,
    log_action_decorator,
    LogUserAction
)

def is_admin(user):
    return user.profile.user_type == 0  # 0 - Администратор

@login_required
@user_passes_test(is_admin, login_url='subscribers:search')
def subscriber_list(request):
    """Представление для просмотра списка абонентов (только для администраторов)"""
    # Получение параметров фильтрации из GET запроса
    search_query = request.GET.get('q', '')
    
    # Базовый QuerySet
    subscribers = Subscriber.objects.all()
    
    # Применение фильтра поиска, если указан
    if search_query:
        subscribers = subscribers.filter(
            models.Q(number__icontains=search_query) |
            models.Q(last_name__icontains=search_query) |
            models.Q(first_name__icontains=search_query) |
            models.Q(middle_name__icontains=search_query) |
            models.Q(address__icontains=search_query) |
            models.Q(imsi__icontains=search_query)
        )
    
    # Сортировка по фамилии и имени
    subscribers = subscribers.order_by('last_name', 'first_name')
    
    # Пагинация
    paginator = Paginator(subscribers, 20)  # 20 записей на страницу
    page = request.GET.get('page')
    subscribers_page = paginator.get_page(page)
    
    return render(request, 'subscribers/subscriber_list.html', {
        'subscribers_page': subscribers_page,
        'search_query': search_query
    })

@login_required
@user_passes_test(is_admin, login_url='subscribers:search')
def import_csv(request):
    """Представление для импорта данных из CSV-файла (только для администраторов)"""
    if request.method == 'POST':
        form = ImportCSVForm(request.POST, request.FILES)
        if form.is_valid():
            try:
                # Получаем данные из формы
                csv_file = request.FILES['csv_file']
                delimiter = form.cleaned_data['delimiter']
                encoding = form.cleaned_data['encoding']
                has_header = form.cleaned_data['has_header']
                
                # Проверяем, что это действительно CSV файл
                if not csv_file.name.lower().endswith('.csv'):
                    messages.error(request, 'Пожалуйста, загрузите файл в формате CSV')
                    return render(request, 'subscribers/import_csv.html', {'form': form})
                
                # Проверяем, что файл не пустой
                if csv_file.size == 0:
                    messages.error(request, 'Загруженный файл пуст')
                    return render(request, 'subscribers/import_csv.html', {'form': form})
                
                # Читаем содержимое файла в виде строки
                try:
                    csv_data = csv_file.read().decode(encoding)
                    
                    # Проверяем, что файл действительно можно прочитать как CSV
                    import csv
                    import io
                    csv_io = io.StringIO(csv_data)
                    reader = csv.reader(csv_io, delimiter=delimiter)
                    
                    # Попытка прочитать первые строки для проверки
                    rows = []
                    for i, row in enumerate(reader):
                        if i >= 5:  # Читаем только первые 5 строк для проверки
                            break
                        rows.append(row)
                    
                    if not rows:
                        messages.error(request, 'Не удалось прочитать данные из CSV файла. Проверьте формат и кодировку.')
                        return render(request, 'subscribers/import_csv.html', {'form': form})
                    
                except UnicodeDecodeError:
                    messages.error(request, f'Не удалось декодировать файл в кодировке {encoding}. Попробуйте другую кодировку.')
                    return render(request, 'subscribers/import_csv.html', {'form': form})
                except Exception as e:
                    messages.error(request, f'Ошибка при обработке файла: {str(e)}')
                    return render(request, 'subscribers/import_csv.html', {'form': form})
                
                # Создаем запись для отслеживания импорта
                import_history = ImportHistory.objects.create(
                    file_name=csv_file.name,
                    file_size=csv_file.size,
                    delimiter=delimiter,
                    encoding=encoding,
                    has_header=has_header,
                    created_by=request.user,
                    status='pending'
                )
                
                # Вызываем функцию импорта напрямую (без Celery)
                from subscribers.tasks import process_csv_import_task_impl
                
                result = process_csv_import_task_impl(
                    csv_data,  # Передаем строковое содержимое CSV
                    import_history.id, 
                    delimiter, 
                    encoding, 
                    has_header, 
                    False  # update_existing=False - заменять все записи новыми
                )
                
                if result and result.get('success', False):
                    messages.success(
                        request, 
                        f'Импорт успешно завершен. Создано: {result["created"]} записей. '
                        f'Ошибок: {result["failed"]} из {result["total"]} записей. '
                        f'Предыдущая таблица архивирована.'
                    )
                    return redirect('subscribers:import_detail', import_id=import_history.id)
                else:
                    messages.error(request, f'Ошибка при импорте: {result.get("error", "Неизвестная ошибка")}')
                    return redirect('subscribers:import_detail', import_id=import_history.id)
                
            except Exception as e:
                messages.error(request, f'Произошла ошибка: {str(e)}')
        else:
            # Форма невалидна, показываем ошибки
            for field, errors in form.errors.items():
                for error in errors:
                    messages.error(request, f'Ошибка в поле {field}: {error}')
    else:
        form = ImportCSVForm()
    
    return render(request, 'subscribers/import_csv.html', {'form': form})

@login_required
@user_passes_test(is_admin, login_url='subscribers:search')
def import_history(request):
    """Представление для просмотра истории импорта (только для администраторов)"""
    history_list = ImportHistory.objects.all().order_by('-created_at')
    
    # Пагинация
    paginator = Paginator(history_list, 10)  # 10 записей на страницу
    page = request.GET.get('page')
    history_page = paginator.get_page(page)
    
    return render(request, 'subscribers/import_history.html', {'history_page': history_page})

@login_required
@user_passes_test(is_admin, login_url='subscribers:search')
def import_detail(request, import_id):
    """Представление для просмотра деталей импорта (только для администраторов)"""
    import_history = get_object_or_404(ImportHistory, id=import_id)
    subscribers = Subscriber.objects.filter(import_history=import_history).order_by('id')
    
    paginator = Paginator(subscribers, 20)
    page_number = request.GET.get('page')
    subscribers_page = paginator.get_page(page_number)
    
    context = {
        'import_history': import_history,
        'subscribers_page': subscribers_page,
    }
    
    return render(request, 'subscribers/import_detail.html', context)

@login_required
@user_passes_test(is_admin, login_url='subscribers:search')
def cleanup_archives(request):
    """Представление для ручной очистки архивных таблиц (только для администраторов)"""
    from subscribers.tasks import cleanup_old_archive_tables
    
    # По умолчанию оставляем 3 последние архивные таблицы
    keep_count = int(request.GET.get('keep', 3))
    
    # Запускаем очистку архивных таблиц
    result = cleanup_old_archive_tables(keep_count)
    
    if result['success']:
        messages.info(
            request, 
            f"Сохранено последних архивных таблиц: {result['total_kept']}. Удалено: {result['total_deleted']}."
        )
    else:
        messages.error(request, f"Ошибка при очистке архивных таблиц: {result.get('error', 'Неизвестная ошибка')}")
    
    # Перенаправляем на страницу истории импорта
    return redirect('subscribers:import_history')

@login_required
def search_subscribers(request):
    """Представление для поиска абонентов"""
    form = SearchForm(request.GET or None)
    subscribers = []
    search_performed = False

    if request.GET and form.is_valid():
        search_performed = True
        phone_number = form.cleaned_data.get('phone_number')
        full_name = form.cleaned_data.get('full_name')
        passport = form.cleaned_data.get('passport')
        address = form.cleaned_data.get('address')
        query = Subscriber.objects.filter(is_active=True)
        if phone_number:
            if len(phone_number) == 11 and phone_number.startswith('993'):
                query = query.filter(number=phone_number)
            else:
                query = query.filter(number__icontains=phone_number)
        if full_name:
            query = query.filter(
                models.Q(first_name__icontains=full_name) |
                models.Q(last_name__icontains=full_name) |
                models.Q(middle_name__icontains=full_name)
            )
        if passport:
            query = query.filter(memo1__icontains=passport)
        if address:
            query = query.filter(address__icontains=address)
        if phone_number or full_name or passport or address:
            subscribers = query.order_by('last_name', 'first_name')
            paginator = Paginator(subscribers, 20)
            page_number = request.GET.get('page')
            subscribers_page = paginator.get_page(page_number)
            subscribers = subscribers_page
            # Логируем только если реально был поиск и есть результаты
            if subscribers:
                log_search(request, request.user, additional_data={
                    'query': request.GET.dict(),
                    'results_count': len(subscribers)
                })
    context = {
        'form': form,
        'subscribers': subscribers,
        'search_performed': search_performed,
    }
    return render(request, 'subscribers/search.html', context)

@login_required
def subscriber_detail(request, subscriber_id):
    """Представление для просмотра подробной информации об абоненте"""
    subscriber = get_object_or_404(Subscriber, id=subscriber_id)
    
    context = {
        'subscriber': subscriber,
    }
    
    return render(request, 'subscribers/subscriber_detail.html', context)

# ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ API ЛОГИРОВАНИЯ - ЗАКОММЕНТИРОВАНЫ ДЛЯ ПРЕДОТВРАЩЕНИЯ ОШИБОК ИМПОРТА
"""
@login_required
def import_subscribers(request):
    if request.method == 'POST':
        form = ImportForm(request.POST, request.FILES)
        if form.is_valid():
            # Логика импорта из CSV
            imported_count = 42  # Количество импортированных записей
            
            # Логируем импорт с дополнительными данными
            log_import(
                request, 
                request.user, 
                additional_data={
                    'file_name': request.FILES['csv_file'].name,
                    'records_count': imported_count
                }
            )
            
            return render(request, 'subscribers/import_success.html', {'count': imported_count})
    else:
        form = ImportForm()
    
    return render(request, 'subscribers/import.html', {'form': form})

@login_required
def export_subscribers(request):
    # Логика экспорта в CSV
    
    # Логируем экспорт с дополнительными данными
    log_export(
        request, 
        request.user, 
        additional_data={
            'format': 'csv',
            'filters': request.GET.dict()
        }
    )
    
    # ... код генерации CSV ...
    return response

@method_decorator(LogUserAction('CREATE'), name='form_valid')
class SubscriberCreateView(LoginRequiredMixin, CreateView):
    model = Subscriber
    form_class = SubscriberForm
    template_name = 'subscribers/form.html'
    success_url = reverse_lazy('subscribers:list')

class SubscriberUpdateView(LoginRequiredMixin, UpdateView):
    model = Subscriber
    form_class = SubscriberForm
    template_name = 'subscribers/form.html'
    success_url = reverse_lazy('subscribers:list')
    
    def form_valid(self, form):
        response = super().form_valid(form)
        
        # Ручное логирование изменения объекта
        log_update(
            self.request, 
            self.request.user, 
            self.object,
            additional_data={
                'changed_fields': form.changed_data
            }
        )
        
        return response

class SubscriberDeleteView(LoginRequiredMixin, DeleteView):
    model = Subscriber
    template_name = 'subscribers/confirm_delete.html'
    success_url = reverse_lazy('subscribers:list')
    
    def delete(self, request, *args, **kwargs):
        subscriber = self.get_object()
        response = super().delete(request, *args, **kwargs)
        
        # Ручное логирование удаления объекта
        log_delete(
            request,
            request.user,
            subscriber,
            additional_data={
                'subscriber_name': f"{subscriber.last_name} {subscriber.first_name}"
            }
        )
        
        return response
"""
