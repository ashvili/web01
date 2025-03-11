from django.shortcuts import render
import csv
import io
import datetime
import logging
import base64
from django.shortcuts import redirect, get_object_or_404
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db import transaction
from django.http import HttpResponse
from django.utils import timezone
from django.urls import reverse
from django.db import models

from .models import Subscriber, ImportHistory
from .forms import CSVImportForm, ImportCSVForm
from .tasks import process_csv_import_task_impl

# Настройка логирования
logger = logging.getLogger(__name__)

@login_required
def subscriber_list(request):
    """Представление для просмотра списка абонентов"""
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
def import_csv(request):
    """Представление для импорта данных из CSV-файла"""
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
def import_history(request):
    """Представление для просмотра истории импорта"""
    history_list = ImportHistory.objects.all().order_by('-created_at')
    
    # Пагинация
    paginator = Paginator(history_list, 10)  # 10 записей на страницу
    page = request.GET.get('page')
    history_page = paginator.get_page(page)
    
    return render(request, 'subscribers/import_history.html', {'history_page': history_page})

@login_required
def import_detail(request, import_id):
    """Представление для просмотра деталей импорта"""
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
def cleanup_archives(request):
    """Представление для ручной очистки архивных таблиц"""
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
