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
from django.http import HttpResponse, JsonResponse
from django.utils import timezone
from django.urls import reverse
from django.db import models
from django.views.generic import ListView, DetailView, CreateView, UpdateView, DeleteView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.utils.decorators import method_decorator
from django.urls import reverse_lazy
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from .models import Subscriber, ImportHistory, ImportError
from .forms import CSVImportForm, SearchForm
from .tasks import process_csv_import_task_impl, start_import_async, is_import_running
from accounts.utils import is_admin

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
        form = CSVImportForm(request.POST, request.FILES)
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
                
                # Создаем запись для отслеживания импорта и сохраняем файл
                import_session_id = f"imp_{timezone.now().strftime('%Y%m%d_%H%M%S')}_{request.user.id}_{csv_file.name[:15].replace(' ', '_')}"
                import_history = ImportHistory.objects.create(
                    file_name=csv_file.name,
                    file_size=csv_file.size,
                    delimiter=delimiter,
                    encoding=encoding,
                    has_header=has_header,
                    created_by=request.user,
                    status='pending',
                    phase='pending',
                    import_session_id=import_session_id
                )
                # присвоим файл
                import_history.uploaded_file = csv_file
                import_history.save()

                # Стартуем асинхронный импорт
                started_now = start_import_async(import_history.id)
                if started_now:
                    messages.info(request, 'Импорт запущен в фоне. Можно следить за прогрессом.')
                else:
                    messages.info(request, 'Импорт уже выполняется.')
                return redirect('subscribers:import_detail', import_id=import_history.id)
                
            except Exception as e:
                messages.error(request, f'Произошла ошибка: {str(e)}')
        else:
            # Форма невалидна, показываем ошибки
            for field, errors in form.errors.items():
                for error in errors:
                    messages.error(request, f'Ошибка в поле {field}: {error}')
    else:
        form = CSVImportForm()
    
    return render(request, 'subscribers/import_csv.html', {'form': form})

@login_required
@user_passes_test(is_admin, login_url='subscribers:search')
@csrf_exempt
def import_csv_async(request):
    """Асинхронное представление для импорта данных из CSV-файла"""
    if request.method == 'POST':
        try:
            # Получаем данные из AJAX запроса
            csv_file = request.FILES.get('csv_file')
            delimiter = request.POST.get('delimiter', ',')
            encoding = request.POST.get('encoding', 'utf-8')
            has_header = request.POST.get('has_header') == 'true'
            
            if not csv_file:
                return JsonResponse({'success': False, 'error': 'Файл не найден'})
            
            # Проверяем, что это действительно CSV файл
            if not csv_file.name.lower().endswith('.csv'):
                return JsonResponse({'success': False, 'error': 'Пожалуйста, загрузите файл в формате CSV'})
            
            # Проверяем, что файл не пустой
            if csv_file.size == 0:
                return JsonResponse({'success': False, 'error': 'Загруженный файл пуст'})
            
            # Создаем запись для отслеживания импорта
            import_session_id = f"imp_{timezone.now().strftime('%Y%m%d_%H%M%S')}_{request.user.id}_{csv_file.name[:15].replace(' ', '_')}"
            import_history = ImportHistory.objects.create(
                file_name=csv_file.name,
                file_size=csv_file.size,
                delimiter=delimiter,
                encoding=encoding,
                has_header=has_header,
                created_by=request.user,
                status='uploading',
                phase='uploading',
                import_session_id=import_session_id
            )
            
            # Сохраняем файл асинхронно
            import_history.uploaded_file = csv_file
            import_history.save()
            
            # Стартуем асинхронный импорт
            started_now = start_import_async(import_history.id)
            
            return JsonResponse({
                'success': True, 
                'import_id': import_history.id,
                'message': 'Импорт запущен в фоне. Можно следить за прогрессом.' if started_now else 'Импорт уже выполняется.'
            })
            
        except Exception as e:
            logger.error(f'Ошибка при асинхронном импорте: {str(e)}')
            return JsonResponse({'success': False, 'error': f'Произошла ошибка: {str(e)}'})
    
    return JsonResponse({'success': False, 'error': 'Метод не поддерживается'})

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
def import_status(request, import_id):
    """JSON-статус прогресса импорта."""
    import_history = get_object_or_404(ImportHistory, id=import_id)
    
    # Добавляем отладочную информацию
    logger.debug(f"Статус импорта {import_id}: {import_history.status}, phase: {getattr(import_history, 'phase', 'N/A')}")
    
    data = {
        'status': import_history.status,
        'phase': getattr(import_history, 'phase', ''),
        'processed': import_history.processed_rows,
        'total': import_history.records_count,
        'records_count': import_history.records_count,  # Добавляем records_count для JavaScript
        'progress_percent': getattr(import_history, 'progress_percent', 0),
        'running': is_import_running(import_history.id),
        'error_message': import_history.error_message or '',
        'pause_requested': getattr(import_history, 'pause_requested', False),
        'cancel_requested': getattr(import_history, 'cancel_requested', False),
        'last_heartbeat_at': import_history.last_heartbeat_at.isoformat() if import_history.last_heartbeat_at else None,
        'stop_reason': getattr(import_history, 'stop_reason', None),
        'errors_count': getattr(import_history, 'errors', None).count() if hasattr(import_history, 'errors') else 0,
        'records_created': import_history.records_created,
        'records_failed': import_history.records_failed,
    }
    
    logger.debug(f"Данные статуса для импорта {import_id}: {data}")
    return JsonResponse(data)

@login_required
@user_passes_test(is_admin, login_url='subscribers:search')
@require_POST
@csrf_exempt
def import_pause(request, import_id):
    import_history = get_object_or_404(ImportHistory, id=import_id)
    logger.info(f"Пауза запрошена для импорта {import_id} пользователем {request.user.username}")
    import_history.pause_requested = True
    import_history.save(update_fields=['pause_requested'])
    return JsonResponse({'ok': True})

@login_required
@user_passes_test(is_admin, login_url='subscribers:search')
@require_POST
@csrf_exempt
def import_resume(request, import_id):
    import_history = get_object_or_404(ImportHistory, id=import_id)
    logger.info(f"Возобновление запрошено для импорта {import_id} пользователем {request.user.username}")
    
    # Сбрасываем флаги паузы и отмены
    import_history.pause_requested = False
    import_history.cancel_requested = False
    
    # Если был в паузе, переведём в pending и запустим
    if import_history.status == 'paused' and not is_import_running(import_history.id):
        import_history.status = 'pending'
        import_history.phase = 'pending'
        import_history.stop_reason = None
        import_history.save(update_fields=['pause_requested', 'cancel_requested', 'status', 'phase', 'stop_reason'])
        started = start_import_async(import_history.id)
        logger.info(f"Импорт {import_id} перезапущен: {started}")
        return JsonResponse({'ok': True, 'started': started})
    
    # Если был в ошибке, перезапускаем с начала
    elif import_history.status == 'failed' and not is_import_running(import_history.id):
        import_history.status = 'pending'
        import_history.phase = 'pending'
        import_history.stop_reason = None
        import_history.error_message = None
        import_history.processed_rows = 0
        import_history.records_created = 0
        import_history.records_failed = 0
        import_history.progress_percent = 0
        import_history.save(update_fields=[
            'pause_requested', 'cancel_requested', 'status', 'phase', 'stop_reason',
            'error_message', 'processed_rows', 'records_created', 'records_failed', 'progress_percent'
        ])
        started = start_import_async(import_history.id)
        logger.info(f"Импорт {import_id} перезапущен после ошибки: {started}")
        return JsonResponse({'ok': True, 'started': started})
    
    # Если импорт уже запущен, просто сбрасываем флаги
    import_history.save(update_fields=['pause_requested', 'cancel_requested'])
    logger.info(f"Флаги сброшены для импорта {import_id}, импорт уже запущен")
    return JsonResponse({'ok': True, 'started': False})

@login_required
@user_passes_test(is_admin, login_url='subscribers:search')
@require_POST
@csrf_exempt
def import_cancel(request, import_id):
    import_history = get_object_or_404(ImportHistory, id=import_id)
    logger.info(f"Отмена запрошена для импорта {import_id} пользователем {request.user.username}")
    import_history.cancel_requested = True
    import_history.save(update_fields=['cancel_requested'])
    return JsonResponse({'ok': True})

@login_required
@user_passes_test(is_admin, login_url='subscribers:search')
@require_POST
@csrf_exempt
def import_finalize(request, import_id):
    """Финализация импорта - перенос данных из временной таблицы в основную"""
    import_history = get_object_or_404(ImportHistory, id=import_id)
    
    # Проверяем, что импорт находится в состоянии temp_completed
    if import_history.status != 'temp_completed':
        return JsonResponse({
            'success': False, 
            'error': f'Импорт должен быть в статусе "temp_completed", текущий статус: {import_history.status}'
        }, status=400)
    
    # Проверяем, что есть временная таблица
    if not import_history.temp_table_name:
        return JsonResponse({
            'success': False, 
            'error': 'Не найдена временная таблица для финализации'
        }, status=400)
    
    try:
        logger.info(f"🏁 Начинаем финализацию импорта {import_id} пользователем {request.user.username}")
        
        # Обновляем статус на финализацию
        import_history.status = 'processing'
        import_history.phase = 'finalizing'
        import_history.save()
        
        # Импортируем функцию финализации
        from .tasks import _finalize_import
        
        # Выполняем финализацию
        _finalize_import(import_history)
        
        # Обновляем статус на завершено
        import_history.status = 'completed'
        import_history.phase = 'completed'
        import_history.save()
        
        logger.info(f"✅ Финализация импорта {import_id} успешно завершена")
        
        return JsonResponse({
            'success': True, 
            'message': 'Импорт успешно финализирован. Данные перенесены в основную таблицу.'
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка при финализации импорта {import_id}: {str(e)}")
        
        # Возвращаем статус обратно в temp_completed при ошибке
        import_history.status = 'temp_completed'
        import_history.phase = 'waiting_finalization'
        import_history.error_message = f"Ошибка при финализации: {str(e)}"
        import_history.save()
        
        return JsonResponse({
            'success': False, 
            'error': f'Ошибка при финализации импорта: {str(e)}'
        }, status=500)

@login_required
@user_passes_test(is_admin, login_url='subscribers:search')
def import_errors(request, import_id):
    """Получение ошибок импорта."""
    try:
        import_history = get_object_or_404(ImportHistory, id=import_id)
        
        # Получаем только ошибки текущего импорта по import_session_id
        # Показываем последние ошибки (недавно созданные), а не первые
        max_errors = min(10, 10)  # Показываем максимум 10 последних ошибок
        errors = ImportError.objects.filter(
            import_session_id=import_history.import_session_id
        ).order_by('-created_at')[:max_errors]  # Сортируем по дате создания (новые сначала)
        
        errors_data = []
        for error in errors:
            errors_data.append({
                'row_index': error.row_index,
                'message': error.message,
                'raw_data': error.raw_data or '',
                'created_at': error.created_at.isoformat() if error.created_at else None
            })
        
        # Возвращаем реальное количество ошибок по import_session_id
        total_errors_count = ImportError.objects.filter(
            import_session_id=import_history.import_session_id
        ).count()
        
        return JsonResponse({
            'success': True,
            'errors': errors_data,
            'total_errors': total_errors_count
        })
    except ImportHistory.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Импорт не найден'}, status=404)
    except Exception as e:
        logger.error(f"Ошибка при получении ошибок импорта {import_id}: {e}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)



@login_required
@user_passes_test(is_admin, login_url='subscribers:search')
def cleanup_archives(request):
    """Представление для ручной очистки архивных таблиц (только для администраторов)"""
    from subscribers.tasks import cleanup_old_archive_tables, list_archive_tables
    from django.http import JsonResponse
    
    # По умолчанию оставляем 3 последние архивные таблицы
    keep_count = int(request.GET.get('keep', 3))
    
    # Сначала получаем информацию о существующих таблицах
    before_info = list_archive_tables()
    
    # Запускаем очистку архивных таблиц
    result = cleanup_old_archive_tables(keep_count)
    
    # Получаем информацию после очистки
    after_info = list_archive_tables()
    
    # Проверяем, является ли это AJAX запросом
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        if result['success']:
            return JsonResponse({
                'success': True,
                'message': f"Сохранено последних архивных таблиц: {result['total_kept']}. Удалено: {result['total_deleted']}.",
                'total_kept': result['total_kept'],
                'total_deleted': result['total_deleted'],
                'before_cleanup': before_info,
                'after_cleanup': after_info
            })
        else:
            return JsonResponse({
                'success': False,
                'error': f"Ошибка при очистке архивных таблиц: {result.get('error', 'Неизвестная ошибка')}",
                'before_cleanup': before_info
            })
    
    # Для обычных запросов (не AJAX) - стандартное поведение
    if result['success']:
        messages.info(
            request, 
            f"Сохранено последних архивных таблиц: {result['total_kept']}. Удалено: {result['total_deleted']}."
        )
    else:
        messages.error(request, f"Ошибка при очистке архивных таблиц: {result.get('error', 'Неизвестная ошибка')}")
    
    # Перенаправляем на страницу истории импорта
    return redirect('subscribers:import_history')

@user_passes_test(is_admin, login_url='subscribers:search')
def list_archives(request):
    """Представление для просмотра списка архивных таблиц (только для администраторов)"""
    from subscribers.tasks import list_archive_tables
    from django.http import JsonResponse
    
    result = list_archive_tables()
    
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return JsonResponse(result)
    
    # Для обычных запросов показываем страницу
    return render(request, 'subscribers/archive_list.html', {'archive_info': result})

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
