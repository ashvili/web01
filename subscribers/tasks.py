import csv
import io
import datetime
import logging
import re
from django.db import transaction, connection
from django.utils import timezone

from .models import Subscriber, ImportHistory

# Настройка логирования
logger = logging.getLogger(__name__)

# Имитация задачи Celery с помощью обычной функции
def process_csv_import_task(csv_data, import_history_id, delimiter, encoding, has_header, update_existing):
    """
    Функция для обработки импорта CSV в базу данных
    
    Args:
        csv_data: Закодированные в base64 данные CSV-файла
        import_history_id: ID записи ImportHistory
        delimiter: Разделитель CSV
        encoding: Кодировка файла
        has_header: Содержит ли CSV заголовок
        update_existing: Обновлять ли существующие записи
    """
    # Имитация асинхронной задачи
    def delay(*args, **kwargs):
        # Выполняем код сразу же, без асинхронности
        return process_csv_import_task_impl(*args, **kwargs)
    
    # Добавляем метод delay к оригинальной функции
    process_csv_import_task.delay = delay
    
    # Выполняем реальную работу
    return process_csv_import_task_impl(csv_data, import_history_id, delimiter, encoding, has_header, update_existing)

def process_csv_import_task_impl(csv_data, import_history_id, delimiter, encoding, has_header, update_existing):
    """
    Обрабатывает импорт данных из CSV.
    Переносит данные из старой таблицы в архивную и заполняет новую.
    """
    try:
        # Получаем запись истории импорта
        import_history = ImportHistory.objects.get(id=import_history_id)
        import_history.status = 'processing'
        import_history.save()
        
        # Создаем новую таблицу с временным именем
        archive_table_name = f"subscribers_subscriber_archive_{int(timezone.now().timestamp())}"
        import_history.archive_table_name = archive_table_name
        import_history.save()
        
        # Предварительная обработка CSV для объединения разделенных строк
        raw_lines = csv_data.splitlines()
        processed_lines = []
        current_line = None
        line_number = 0
        id_pattern = re.compile(r'^\s*\d+')  # Проверка, начинается ли строка с числа (ID)
        
        # Анализ первых строк CSV для определения проблем
        sample_rows = []
        for i, line in enumerate(raw_lines[:10]):  # Анализируем первые 10 строк
            if i == 0 and has_header:  # Пропускаем заголовок
                continue
            if not line.strip():  # Пропускаем пустые строки
                continue
            if id_pattern.match(line.split(delimiter)[0]):  # Только если строка - это запись (начинается с ID)
                parts = line.split(delimiter)
                sample_rows.append(parts)
                if len(parts) > 9:  # Если есть колонка даты
                    print(f"Пример даты в строке {i+1}: '{parts[9]}'")
        
        # Статистика по столбцам
        if sample_rows:
            print("\nАнализ структуры CSV:")
            max_cols = max(len(row) for row in sample_rows)
            for col_idx in range(max_cols):
                non_empty_count = sum(1 for row in sample_rows if col_idx < len(row) and row[col_idx].strip())
                if col_idx == 9:  # Колонка даты рождения
                    print(f"Колонка {col_idx+1} (предполагаемая дата рождения): {non_empty_count}/{len(sample_rows)} непустых значений")
                    # Примеры значений
                    examples = [row[col_idx] for row in sample_rows if col_idx < len(row) and row[col_idx].strip()]
                    if examples:
                        print(f"Примеры значений: {examples[:5]}")
        
        for line in raw_lines:
            line_number += 1
            
            # Пропускаем пустые строки
            if not line.strip():
                continue
            
            # Пропускаем первую строку с заголовком, если он есть
            if line_number == 1 and has_header:
                processed_lines.append(line)
                continue
            
            # Проверяем, начинается ли строка с ID (числа)
            is_new_record = bool(id_pattern.match(line.split(delimiter)[0]))
            
            if is_new_record:
                # Если есть текущая строка, добавляем ее в обработанные
                if current_line is not None:
                    processed_lines.append(current_line)
                # Начинаем новую строку
                current_line = line
            else:
                # Это продолжение предыдущей строки
                if current_line is not None:
                    # Объединяем с текущей строкой
                    current_line = current_line + " " + line.strip()
                else:
                    # Если это первая строка и она не начинается с ID - пропускаем или предупреждаем
                    if not has_header or line_number > 1:
                        import_history.error_message = f"Предупреждение: строка {line_number} не начинается с ID и не имеет предшествующей записи. Строка пропущена."
                        import_history.save()
        
        # Добавляем последнюю обработанную строку
        if current_line is not None:
            processed_lines.append(current_line)
        
        # Собираем обработанные строки обратно в одну строку
        processed_csv_data = "\n".join(processed_lines)
        
        # Чтение обработанного CSV-файла
        csv_file = io.StringIO(processed_csv_data)
        
        # Используем правильные настройки CSV-reader для обработки кавычек
        csv_reader = csv.reader(
            csv_file, 
            delimiter=delimiter, 
            quotechar='"', 
            quoting=csv.QUOTE_MINIMAL
        )
        
        # Пропускаем первую строку, если есть заголовок
        if has_header:
            next(csv_reader, None)
        
        # Список для хранения обработанных записей
        parsed_rows = []
        errors = []
        row_count = 0
        
        for row in csv_reader:
            row_count += 1
            
            if len(row) < 8:  # Минимальное количество полей
                errors.append(f"Строка {row_count}: неверное количество полей ({len(row)})")
                continue
                
            try:
                # Разбираем строку CSV
                original_id_str = row[0].strip() if row[0] else None
                original_id = None
                
                if original_id_str:
                    try:
                        original_id = int(original_id_str)
                    except ValueError:
                        errors.append(f"Некорректный ID в строке {row_count}: {original_id_str}")
                
                number = row[1].strip() if len(row) > 1 else ""
                last_name = row[2].strip() if len(row) > 2 else ""
                first_name = row[3].strip() if len(row) > 3 else ""
                middle_name = row[4].strip() if len(row) > 4 else None
                address = row[5].strip() if len(row) > 5 else None
                memo1 = row[6].strip() if len(row) > 6 else None
                memo2 = row[7].strip() if len(row) > 7 else None
                birth_place = row[8].strip() if len(row) > 8 else None
                
                birth_date = None
                if len(row) > 9 and row[9] and row[9].strip():
                    try:
                        # Получаем строку с датой
                        from datetime import datetime, date
                        birth_date_str = row[9].strip()
                        
                        # Логирование для отладки
                        print(f"Обработка даты: '{birth_date_str}' в строке {row_count}")
                        
                        # Пропускаем NULL значение (обрабатываем как None)
                        if birth_date_str.upper() == 'NULL':
                            print(f"Найдено значение NULL, устанавливаем date как None")
                            birth_date = None
                        else:
                            # Для формата с датой и временем (YYYY-MM-DD 00:00:00.000)
                            # Сначала отрезаем время, если оно есть
                            if ' ' in birth_date_str:
                                date_part = birth_date_str.split(' ')[0]
                                print(f"Отделена часть с датой: {date_part}")
                            else:
                                date_part = birth_date_str
                                
                            # Теперь разбираем только часть с датой
                            if '-' in date_part:
                                parts = date_part.split('-')
                                if len(parts) == 3:
                                    year = int(parts[0])
                                    month = int(parts[1])
                                    day = int(parts[2])
                                    
                                    # Проверка валидности даты
                                    if 1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2100:
                                        try:
                                            birth_date = date(year, month, day)
                                            print(f"Дата успешно преобразована: {birth_date}")
                                        except ValueError as ve:
                                            print(f"Ошибка при создании даты: {ve}")
                                            errors.append(f"Некорректная дата '{birth_date_str}' в строке {row_count}: {ve}")
                                    else:
                                        print(f"Неверные значения дня ({day}), месяца ({month}) или года ({year})")
                                        errors.append(f"Неверные значения дня, месяца или года в дате '{birth_date_str}' (строка {row_count})")
                                else:
                                    print(f"Неверное количество частей в дате: {parts}")
                                    errors.append(f"Неверный формат даты '{birth_date_str}' в строке {row_count}")
                            else:
                                # Пробуем использовать стандартные функции парсинга даты
                                try:
                                    parsed_date = datetime.strptime(birth_date_str, '%Y-%m-%d %H:%M:%S.%f')
                                    birth_date = parsed_date.date()
                                    print(f"Дата успешно преобразована через strptime: {birth_date}")
                                except ValueError:
                                    try:
                                        # Пробуем более простой формат
                                        parsed_date = datetime.strptime(birth_date_str, '%Y-%m-%d')
                                        birth_date = parsed_date.date()
                                        print(f"Дата успешно преобразована через strptime (простой формат): {birth_date}")
                                    except ValueError as ve:
                                        print(f"Не удалось разобрать дату '{birth_date_str}': {ve}")
                                        errors.append(f"Не удалось разобрать дату '{birth_date_str}' в строке {row_count}")
                    except Exception as e:
                        errors.append(f"Ошибка при обработке даты рождения в строке {row_count}: {str(e)}")
                        print(f"Неожиданная ошибка при обработке даты: {str(e)}")
                
                imsi = row[10].strip() if len(row) > 10 else None
                
                # Проверка на пустые значения обязательных полей
                if not last_name or not first_name:
                    errors.append(f"Строка {row_count}: отсутствуют обязательные поля (фамилия или имя)")
                    continue
                
                # Добавляем строку в список обработанных
                parsed_rows.append({
                    'original_id': original_id,
                    'number': number,
                    'last_name': last_name,
                    'first_name': first_name,
                    'middle_name': middle_name,
                    'address': address,
                    'memo1': memo1,
                    'memo2': memo2,
                    'birth_place': birth_place,
                    'birth_date': birth_date,
                    'imsi': imsi
                })
                
            except Exception as e:
                errors.append(f"Ошибка при обработке строки {row_count}: {str(e)}")
        
        # Архивируем существующую таблицу и создаём новую с чистыми данными
        with connection.cursor() as cursor:
            try:
                # 1. Создаём архивную таблицу
                cursor.execute(f"""
                    CREATE TABLE {archive_table_name} AS 
                    SELECT * FROM subscribers_subscriber
                """)
                
                # 2. Очищаем существующую таблицу
                cursor.execute("DELETE FROM subscribers_subscriber")
                
                # Обновляем информацию в историю импорта
                import_history.archive_table_name = archive_table_name
                import_history.records_count = len(parsed_rows)
                
                if errors:
                    error_message = "\n".join(errors[:20])
                    if len(errors) > 20:
                        error_message += f"\n... ещё {len(errors) - 20} ошибок"
                    import_history.error_message = error_message
                    
                import_history.save()
                
            except Exception as e:
                import_history.status = 'failed'
                import_history.error_message = f"Ошибка при архивации данных: {str(e)}"
                import_history.save()
                return {"success": False, "error": str(e)}
        
        # Вставляем новые записи без использования глобальной транзакции
        created_count = 0
        failed_count = 0
        
        for record in parsed_rows:
            try:
                # Логирование для отладки
                if record['birth_date'] is not None:
                    from datetime import date
                    print(f"Сохранение записи с датой рождения: {record['birth_date']} (тип: {type(record['birth_date']).__name__})")
                    
                    # Если birth_date не является объектом типа date, сконвертируем его
                    if not isinstance(record['birth_date'], date):
                        if hasattr(record['birth_date'], 'date'):  # Если это datetime
                            record['birth_date'] = record['birth_date'].date()
                            print(f"Преобразовано в date: {record['birth_date']}")
                
                # Каждая запись в своей транзакции
                with transaction.atomic():
                    new_subscriber = Subscriber(
                        original_id=record['original_id'],
                        number=record['number'],
                        last_name=record['last_name'],
                        first_name=record['first_name'],
                        middle_name=record['middle_name'],
                        address=record['address'],
                        memo1=record['memo1'],
                        memo2=record['memo2'],
                        birth_place=record['birth_place'],
                        birth_date=record['birth_date'],
                        imsi=record['imsi'],
                        import_history=import_history
                    )
                    new_subscriber.save()
                    
                    # Проверяем, сохранилась ли дата корректно
                    if record['birth_date'] is not None:
                        saved_sub = Subscriber.objects.get(pk=new_subscriber.pk)
                        if saved_sub.birth_date is None:
                            print(f"ВНИМАНИЕ: Дата рождения не сохранилась для абонента {new_subscriber.pk}")
                        else:
                            print(f"Дата рождения сохранена успешно: {saved_sub.birth_date}")
                    
                    created_count += 1
            except Exception as e:
                failed_count += 1
                errors.append(f"Ошибка при создании записи: {str(e)}")
                print(f"Ошибка при сохранении абонента: {str(e)}")
        
        # Обновляем статистику импорта
        import_history.records_created = created_count
        import_history.records_failed = failed_count
        import_history.status = 'completed'
        
        # Обновляем сообщение об ошибках, если они есть
        if errors:
            error_message = "\n".join(errors[:20])
            if len(errors) > 20:
                error_message += f"\n... ещё {len(errors) - 20} ошибок"
            import_history.error_message = error_message
            
        import_history.save()
        
        # Удаляем старые архивные таблицы, оставляя только последние 3
        try:
            print("Запускаем очистку старых архивных таблиц...")
            with connection.cursor() as cursor:
                # Получаем список всех таблиц в базе данных
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_name LIKE 'subscribers_subscriber_archive_%'
                    ORDER BY table_name DESC
                """)
                archive_tables = [row[0] for row in cursor.fetchall()]
                print(f"Найдено архивных таблиц: {len(archive_tables)}")
                
                # Оставляем только 3 последние таблицы (включая текущую)
                tables_to_keep = 3
                tables_to_delete = archive_tables[tables_to_keep:]
                
                # Удаляем устаревшие таблицы
                for table in tables_to_delete:
                    print(f"Удаление устаревшей архивной таблицы: {table}")
                    cursor.execute(f"DROP TABLE IF EXISTS {table}")
                
                print(f"Удалено архивных таблиц: {len(tables_to_delete)}")
                
                # Обновляем информацию об архивации в сообщении
                saved_tables = ", ".join(archive_tables[:tables_to_keep])
                cleanup_info = f"Сохранено последних архивных таблиц: {min(tables_to_keep, len(archive_tables))}. Удалено: {len(tables_to_delete)}."
                
                # Сохраняем информацию об очистке в info_message
                import_history.info_message = cleanup_info
                import_history.save()
                
                # Добавляем информацию об очистке к ошибкам, если они есть
                if import_history.error_message:
                    import_history.error_message += f"\n\n{cleanup_info}"
                else:
                    import_history.error_message = cleanup_info
                
                import_history.save()
        except Exception as e:
            print(f"Ошибка при очистке старых архивных таблиц: {str(e)}")
            import_history.error_message = f"Ошибка при очистке старых архивных таблиц: {str(e)}"
            import_history.save()
        
        return {
            "success": True,
            "created": created_count,
            "failed": failed_count,
            "total": row_count,
            "archive_table": archive_table_name
        }
        
    except Exception as e:
        # В случае неожиданной ошибки обновляем статус импорта
        try:
            import_history = ImportHistory.objects.get(id=import_history_id)
            import_history.status = 'failed'
            import_history.error_message = f"Непредвиденная ошибка: {str(e)}"
            import_history.save()
        except:
            pass
        
        return {"success": False, "error": str(e)}

# Имитация задачи Celery для очистки устаревших данных
def cleanup_old_import_data(days=30):
    """
    Функция для очистки устаревших данных импорта
    
    Args:
        days: Количество дней, после которых данные считаются устаревшими
    """
    # Имитация асинхронной задачи
    def delay(*args, **kwargs):
        # Выполняем код сразу же, без асинхронности
        return cleanup_old_import_data_impl(*args, **kwargs)
    
    # Добавляем метод delay к оригинальной функции
    cleanup_old_import_data.delay = delay
    
    # Выполняем реальную работу
    return cleanup_old_import_data_impl(days)

def cleanup_old_import_data_impl(days=30):
    """Реализация задачи очистки устаревших данных"""
    threshold_date = timezone.now() - datetime.timedelta(days=days)
    
    try:
        # Получаем все устаревшие импорты
        old_imports = ImportHistory.objects.filter(created_at__lt=threshold_date)
        
        import_count = old_imports.count()
        
        if import_count > 0:
            # Логируем информацию о начале очистки
            logger.info(f"Начало очистки старых данных импорта: {import_count} записей")
            
            # Находим архивные таблицы абонентов, которые старше threshold_date
            with connection.cursor() as cursor:
                # Получаем список всех таблиц в базе данных
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name LIKE 'subscribers_subscriber_archive_%'
                """)
                archive_tables = cursor.fetchall()
                
                # Удаляем старые архивные таблицы
                for table in archive_tables:
                    table_name = table[0]
                    # Извлекаем дату из имени таблицы
                    try:
                        date_part = table_name.split('_')[-2:]
                        table_date_str = f"{date_part[0]}_{date_part[1]}"
                        table_date = datetime.datetime.strptime(table_date_str, "%Y%m%d_%H%M%S")
                        
                        # Если таблица старше threshold_date, удаляем её
                        if table_date.replace(tzinfo=timezone.utc) < threshold_date:
                            logger.info(f"Удаляем архивную таблицу: {table_name}")
                            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                    except (IndexError, ValueError) as e:
                        logger.warning(f"Невозможно определить дату для таблицы {table_name}: {str(e)}")
            
            # Получаем абонентов, связанных с устаревшими импортами
            old_subscribers = Subscriber.objects.filter(import_history__in=old_imports)
            subscriber_count = old_subscribers.count()
            
            # Удаляем абонентов
            old_subscribers.delete()
            
            # Удаляем импорты
            old_imports.delete()
            
            logger.info(f"Очистка завершена: удалено {import_count} записей импорта и {subscriber_count} абонентов")
            return f"Очистка завершена: удалено {import_count} записей импорта и {subscriber_count} абонентов, а также старые архивные таблицы"
        else:
            logger.info("Нет устаревших данных для очистки")
            return "Нет устаревших данных для очистки"
    
    except Exception as e:
        logger.error(f"Ошибка при очистке устаревших данных: {str(e)}")
        return f"Ошибка при очистке устаревших данных: {str(e)}"

def cleanup_old_archive_tables(keep_count=3):
    """
    Функция для очистки старых архивных таблиц.
    Оставляет только указанное количество последних архивных таблиц.
    
    Args:
        keep_count (int): Количество последних таблиц, которые нужно сохранить
        
    Returns:
        dict: Результат выполнения функции
    """
    from django.db import connection
    
    try:
        print(f"Запускаем очистку старых архивных таблиц, оставляем {keep_count} последних...")
        with connection.cursor() as cursor:
            # Получаем список всех таблиц в базе данных
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name LIKE 'subscribers_subscriber_archive_%'
                ORDER BY table_name DESC
            """)
            archive_tables = [row[0] for row in cursor.fetchall()]
            print(f"Найдено архивных таблиц: {len(archive_tables)}")
            
            # Определяем таблицы для удаления
            tables_to_delete = archive_tables[keep_count:] if len(archive_tables) > keep_count else []
            
            # Удаляем устаревшие таблицы
            deleted_tables = []
            for table in tables_to_delete:
                print(f"Удаление устаревшей архивной таблицы: {table}")
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                deleted_tables.append(table)
            
            print(f"Удалено архивных таблиц: {len(deleted_tables)}")
            return {
                "success": True,
                "kept_tables": archive_tables[:keep_count],
                "deleted_tables": deleted_tables,
                "total_kept": min(keep_count, len(archive_tables)),
                "total_deleted": len(deleted_tables)
            }
    except Exception as e:
        print(f"Ошибка при очистке старых архивных таблиц: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

def cleanup_old_archive_tables_task(keep_count=3):
    """
    Задача очистки старых архивных таблиц.
    Оставляет только указанное количество последних архивных таблиц.
    """
    # Имитация асинхронной задачи
    def delay(*args, **kwargs):
        # Выполняем код сразу же, без асинхронности
        return cleanup_old_archive_tables(*args, **kwargs)
    
    # Добавляем метод delay к оригинальной функции
    cleanup_old_archive_tables_task.delay = delay
    
    # Выполняем реальную работу
    return cleanup_old_archive_tables(keep_count) 