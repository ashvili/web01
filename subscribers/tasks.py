import csv
import io
import datetime
import logging
import re
import threading
from pathlib import Path
from typing import Optional
from django.conf import settings
from django.db import transaction, connection
from django.utils import timezone

from .models import Subscriber, ImportHistory, ImportError

# Настройка логирования
logger = logging.getLogger(__name__)

# Регистр активных импортов, чтобы не запускать параллельно один и тот же
_RUNNING_IMPORTS = {}

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

# === РЕЖИМ ПОТОКОВОГО (РЕЗЮМИРУЕМОГО) ИМПОРТА ===

def _count_total_records(file_path: Path, delimiter: str, has_header: bool) -> int:
    """Быстрый подсчёт числа записей в CSV по признаку ID в первом столбце."""
    id_pattern = re.compile(r'^\s*\d+')
    total = 0
    with file_path.open('r', encoding='utf-8', errors='ignore') as fh:
        for idx, line in enumerate(fh, start=1):
            if idx == 1 and has_header:
                continue
            if not line.strip():
                continue
            first_col = line.split(delimiter)[0]
            if id_pattern.match(first_col):
                total += 1
    return total

def _process_record_row(parsed, import_history: ImportHistory, created_failed_acc):
    created_count, failed_count, errors = created_failed_acc
    try:
        # ПРОВЕРКА ФЛАГОВ ПРЯМО ПЕРЕД СОХРАНЕНИЕМ ЗАПИСИ
        import_history.refresh_from_db(fields=['pause_requested', 'cancel_requested'])
        if import_history.cancel_requested or import_history.pause_requested:
            # Если запрошена пауза или отмена, просто возвращаем текущие счетчики
            # Основной цикл обработает эти флаги
            return created_count, failed_count, errors
        
        # Нормализация даты
        if parsed['birth_date'] is not None:
            from datetime import date
            if not isinstance(parsed['birth_date'], date) and hasattr(parsed['birth_date'], 'date'):
                parsed['birth_date'] = parsed['birth_date'].date()

        with transaction.atomic():
            new_subscriber = Subscriber(
                original_id=parsed['original_id'],
                number=parsed['number'],
                last_name=parsed['last_name'],
                first_name=parsed['first_name'],
                middle_name=parsed['middle_name'],
                address=parsed['address'],
                memo1=parsed['memo1'],
                memo2=parsed['memo2'],
                birth_place=parsed['birth_place'],
                birth_date=parsed['birth_date'],
                imsi=parsed['imsi'],
                import_history=import_history,
            )
            new_subscriber.save()
        created_count += 1
    except Exception as e:  # noqa: BLE001 - логируем и продолжаем
        failed_count += 1
        error_msg = f"Ошибка при создании записи: {str(e)}"
        errors.append(error_msg)
        # Сохраняем исходные данные для анализа
        raw_data = f"ID: {parsed.get('original_id', 'N/A')}, Номер: {parsed.get('number', 'N/A')}, ФИО: {parsed.get('last_name', 'N/A')} {parsed.get('first_name', 'N/A')} {parsed.get('middle_name', 'N/A')}, Адрес: {parsed.get('address', 'N/A')}, Дата: {parsed.get('birth_date', 'N/A')}"
        ImportError.objects.create(
            import_history=import_history,
            import_session_id=import_history.import_session_id,
            row_index=import_history.processed_rows + 1,
            message=error_msg,
            raw_data=raw_data
        )
    return created_count, failed_count, errors

def _extract_id_from_line(line, delimiter):
    """Извлекает ID из первого поля строки."""
    if not line or not line.strip():
        return None
    
    try:
        first_field = line.split(delimiter)[0].strip()
        return int(first_field) if first_field else None
    except (ValueError, IndexError):
        return None

def _is_valid_id_field_value(id_value, expected_id=None):
    """Проверяет, является ли ID корректным значением."""
    if id_value is None or id_value <= 0:
        return False
    
    if expected_id is not None:
        # ID должен быть больше ожидаемого (разумная последовательность)
        if id_value <= expected_id:
            return False
        # ID не должен сильно отличаться от ожидаемого (разница не более 1000)
        if id_value - expected_id > 1000:
            return False
    
    return True

def _is_valid_id_field(field_value, expected_id=None):
    """Проверяет, является ли первое поле корректным ID."""
    if not field_value or not field_value.strip():
        return False
    
    try:
        parsed_id = int(field_value.strip())
        return _is_valid_id_field_value(parsed_id, expected_id)
    except ValueError:
        return False

def _try_parse_csv_line(line, delimiter):
    """Пробует распарсить строку как CSV и вернуть поля."""
    try:
        import csv
        import io
        csv_io = io.StringIO(line)
        reader = csv.reader(csv_io, delimiter=delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        return next(reader, None)
    except Exception:
        return None

# Старые функции удалены - теперь используется новый алгоритм с предпросмотром

def _try_process_combined_line(combined_line, logical_row_index, delimiter, import_history):
    """
    Пытается обработать объединенную строку как CSV запись.
    
    Returns:
        (success, actual_id) - success указывает на успех, actual_id - фактический ID записи
    """
    errors = []
    try:
        # Пытаемся распарсить объединенную строку
        row_values = _try_parse_csv_line(combined_line, delimiter)
        if not row_values:
            return False, None
        
        # Проверяем, что есть достаточно полей
        if len(row_values) < 8:
            return False, None
        
        # Получаем фактический ID
        actual_id = None
        if row_values[0] and row_values[0].strip():
            try:
                actual_id = int(row_values[0].strip())
            except ValueError:
                return False, None
        
        # Парсим запись
        parsed = _parse_line_to_record(row_values, logical_row_index, errors)
        if not parsed:
            return False, None
        
        # Пытаемся сохранить запись
        try:
            created_count, failed_count, errors = _process_record_row(parsed, import_history, (0, 0, errors))
            return failed_count == 0, actual_id
        except Exception:
            return False, None
            
    except Exception:
        return False, None

def _parse_line_to_record(row_values, row_count, errors):
    """Преобразование массива строк в словарь полей."""
    try:
        if len(row_values) < 8:
            errors.append(f"Строка {row_count}: неверное количество полей ({len(row_values)})")
            return None
        original_id = None
        original_id_str = row_values[0].strip() if row_values[0] else None
        if original_id_str:
            try:
                original_id = int(original_id_str)
            except ValueError:
                errors.append(f"Некорректный ID в строке {row_count}: {original_id_str}")
        number = row_values[1].strip() if len(row_values) > 1 else ""
        last_name = row_values[2].strip() if len(row_values) > 2 else ""
        first_name = row_values[3].strip() if len(row_values) > 3 else ""
        middle_name = row_values[4].strip() if len(row_values) > 4 else None
        address = row_values[5].strip() if len(row_values) > 5 else None
        memo1 = row_values[6].strip() if len(row_values) > 6 else None
        memo2 = row_values[7].strip() if len(row_values) > 7 else None
        birth_place = row_values[8].strip() if len(row_values) > 8 else None
        imsi = row_values[10].strip() if len(row_values) > 10 else None

        # Дата рождения
        birth_date = None
        if len(row_values) > 9 and row_values[9] and row_values[9].strip():
            from datetime import datetime, date
            birth_date_str = row_values[9].strip()
            if birth_date_str.upper() == 'NULL':
                birth_date = None
            else:
                if ' ' in birth_date_str:
                    date_part = birth_date_str.split(' ')[0]
                else:
                    date_part = birth_date_str
                if '-' in date_part:
                    parts = date_part.split('-')
                    if len(parts) == 3:
                        year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                        if 1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2100:
                            try:
                                birth_date = date(year, month, day)
                            except ValueError as ve:
                                errors.append(f"Некорректная дата '{birth_date_str}' в строке {row_count}: {ve}")
                        else:
                            errors.append(f"Неверные значения дня/месяца/года в дате '{birth_date_str}' (строка {row_count})")
                    else:
                        errors.append(f"Неверный формат даты '{birth_date_str}' в строке {row_count}")
                else:
                    try:
                        parsed_date = datetime.strptime(birth_date_str, '%Y-%m-%d %H:%M:%S.%f')
                        birth_date = parsed_date.date()
                    except ValueError:
                        try:
                            parsed_date = datetime.strptime(birth_date_str, '%Y-%m-%d')
                            birth_date = parsed_date.date()
                        except ValueError:
                            errors.append(f"Не удалось разобрать дату '{birth_date_str}' в строке {row_count}")

        if not last_name or not first_name:
            errors.append(f"Строка {row_count}: отсутствуют обязательные поля (фамилия или имя)")
            return None

        return {
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
            'imsi': imsi,
        }
    except Exception as e:  # noqa: BLE001
        errors.append(f"Ошибка при обработке строки {row_count}: {str(e)}")
        return None

def _process_csv_lines_with_smart_joining(file_path, delimiter, encoding, has_header, import_history, processed_rows_start):
    """
    Обрабатывает CSV файл с умным склеиванием разбитых строк.
    Использует предпросмотр следующей строки для принятия решения о склеивании.
    
    Returns:
        (created_count, failed_count, last_processed_row)
    """
    created_count = 0
    failed_count = 0
    logical_row_index = processed_rows_start
    
    expected_id = None
    last_valid_line = None  # Последняя строка с правильным полем
    
    with file_path.open('r', encoding=encoding, errors='ignore') as fh:
        import_history.phase = 'processing'
        import_history.save()
        
        # Читаем все строки сразу для возможности предпросмотра
        all_lines = [line.rstrip('\n\r') for line in fh.readlines()]
        
        physical_line_idx = 0
        i = 0
        
        while i < len(all_lines):
            # Heartbeat
            import_history.last_heartbeat_at = timezone.now()
            if logical_row_index % 50 == 0:
                import_history.save(update_fields=['last_heartbeat_at'])

            # Управление: пауза / отмена
            import_history.refresh_from_db(fields=['pause_requested', 'cancel_requested'])
            
            if import_history.cancel_requested:
                logger.info(f"Импорт {import_history.id} отменен пользователем")
                import_history.status = 'cancelled'
                import_history.stop_reason = 'Отмена пользователем'
                import_history.phase = 'completed'
                import_history.save()
                return created_count, failed_count, logical_row_index
                
            if import_history.pause_requested:
                logger.info(f"Импорт {import_history.id} поставлен на паузу пользователем")
                import_history.status = 'paused'
                import_history.stop_reason = 'Пауза пользователем'
                import_history.save()
                # Ожидаем снятия паузы
                while True:
                    import_history.refresh_from_db(fields=['pause_requested', 'cancel_requested'])
                    if import_history.cancel_requested:
                        logger.info(f"Импорт {import_history.id} отменен во время паузы")
                        import_history.status = 'cancelled'
                        import_history.stop_reason = 'Отмена пользователем'
                        import_history.phase = 'completed'
                        import_history.save()
                        return created_count, failed_count, logical_row_index
                    if not import_history.pause_requested:
                        logger.info(f"Импорт {import_history.id} возобновлен после паузы")
                        import_history.status = 'processing'
                        import_history.stop_reason = None
                        import_history.save()
                        break
                    import time
                    time.sleep(0.5)
            
            current_line = all_lines[i].strip()
            physical_line_idx += 1
            
            # Пропускаем заголовок
            if physical_line_idx == 1 and has_header:
                i += 1
                continue
                
            # Пропускаем пустые строки
            if not current_line:
                i += 1
                continue
            
            # Проверяем, является ли текущая строка валидной (начинается с правильного ID)
            current_id = _extract_id_from_line(current_line, delimiter)
            # Для первой записи не проверяем expected_id
            is_current_valid = _is_valid_id_field_value(current_id, expected_id if expected_id is not None else None)
            
            if is_current_valid:
                # Текущая строка валидная - сохраняем как последнюю валидную
                last_valid_line = current_line
                
                # Смотрим следующие строки для склеивания
                combined_line = current_line
                lines_to_combine = [current_line]
                j = i + 1
                
                # Ищем следующую валидную строку или достигаем конца файла
                while j < len(all_lines):
                    next_line = all_lines[j].strip()
                    
                    # Пропускаем пустые строки
                    if not next_line:
                        j += 1
                        continue
                    
                    next_id = _extract_id_from_line(next_line, delimiter)
                    is_next_valid = _is_valid_id_field_value(next_id, current_id)
                    
                    if is_next_valid:
                        # Следующая строка валидная - прекращаем склеивание
                        break
                    else:
                        # Следующая строка не валидная - добавляем к текущей
                        combined_line += " " + next_line
                        lines_to_combine.append(next_line)
                        j += 1
                
                # Пытаемся обработать объединенную строку
                logical_row_index += 1
                if logical_row_index > processed_rows_start:
                    success, actual_id = _try_process_combined_line(
                        combined_line, logical_row_index, delimiter, import_history
                    )
                    
                    if success:
                        created_count += 1
                        expected_id = actual_id
                    else:
                        failed_count += 1
                        # Записываем ошибку с подробными исходными данными
                        raw_data_lines = []
                        
                        # Записываем строки, начиная с последней валидной строки
                        if last_valid_line and last_valid_line != current_line:
                            raw_data_lines.append(f"Последняя валидная строка: {last_valid_line}")
                        
                        # Записываем все строки, которые пытались склеить
                        for idx, line in enumerate(lines_to_combine):
                            if idx == 0:
                                raw_data_lines.append(f"Текущая строка (начало записи): {line}")
                            else:
                                raw_data_lines.append(f"Продолжение строки {idx}: {line}")
                        
                        # Добавляем результат склеивания
                        raw_data_lines.append(f"Результат склеивания: {combined_line}")
                        
                        # Добавляем диагностическую информацию
                        row_values = _try_parse_csv_line(combined_line, delimiter)
                        if row_values:
                            raw_data_lines.append(f"Количество полей после парсинга: {len(row_values)}")
                            if len(row_values) > 0:
                                raw_data_lines.append(f"Первое поле: '{row_values[0]}'")
                        else:
                            raw_data_lines.append("Не удалось распарсить как CSV")
                        
                        ImportError.objects.create(
                            import_history=import_history,
                            import_session_id=import_history.import_session_id,
                            row_index=logical_row_index,
                            message="Не удалось восстановить разбитую запись",
                            raw_data="\n".join(raw_data_lines)[:2000]  # Увеличиваем размер до 2000 символов
                        )
                    
                    # Обновляем прогресс
                    import_history.processed_rows = logical_row_index
                    import_history.records_created = created_count
                    import_history.records_failed = failed_count
                    if import_history.records_count:
                        pct = int((logical_row_index / import_history.records_count) * 100)
                        import_history.progress_percent = min(pct, 100)
                    
                    if logical_row_index % 10 == 0:
                        import_history.save()
                
                # Переходим к следующей непроверенной строке
                i = j
            else:
                # Текущая строка не валидная - пропускаем (такого не должно быть при правильной логике)
                i += 1
    
    return created_count, failed_count, logical_row_index

def _process_single_csv_record(line, logical_row_index, delimiter, import_history, expected_id=None):
    """
    Обрабатывает одну CSV запись.
    
    Returns:
        (created_count, failed_count, actual_id)
    """
    errors = []
    created_count = 0
    failed_count = 0
    actual_id = expected_id
    
    try:
        row_values = _try_parse_csv_line(line, delimiter)
        if row_values:
            # Получаем фактический ID для следующей проверки
            if row_values[0] and row_values[0].strip():
                try:
                    actual_id = int(row_values[0].strip())
                except ValueError:
                    pass
            
            parsed = _parse_line_to_record(row_values, logical_row_index, errors)
            if parsed:
                try:
                    created_count, failed_count, errors = _process_record_row(parsed, import_history, (created_count, failed_count, errors))
                except Exception as e:
                    failed_count += 1
                    msg = f"Не удалось сохранить запись: {str(e)}"
                    errors.append(msg)
                    ImportError.objects.create(
                        import_history=import_history,
                        import_session_id=import_history.import_session_id,
                        row_index=logical_row_index,
                        message=msg,
                        raw_data=line[:500]
                    )
            else:
                # Ошибка парсинга
                if errors:
                    failed_count += 1
                    ImportError.objects.create(
                        import_history=import_history,
                        import_session_id=import_history.import_session_id,
                        row_index=logical_row_index,
                        message=errors[-1],
                        raw_data=line[:500]
                    )
    except Exception as e:
        failed_count += 1
        ImportError.objects.create(
            import_history=import_history,
            import_session_id=import_history.import_session_id,
            row_index=logical_row_index,
            message=f"Ошибка обработки строки: {str(e)}",
            raw_data=line[:500]
        )
    
    return created_count, failed_count, actual_id

def process_csv_import_stream(import_history_id: int) -> None:
    """Потоковый импорт с возможностью резюме по ImportHistory.processed_rows."""
    import_history = ImportHistory.objects.get(id=import_history_id)
    logger.info(f"Запуск потокового импорта {import_history_id}, текущий статус: {import_history.status}")
    
    # Если импорт был в паузе, продолжаем с того места, где остановились
    if import_history.status == 'paused':
        logger.info(f"Возобновляем импорт {import_history_id} с позиции {import_history.processed_rows}")
        import_history.status = 'processing'
        import_history.phase = 'processing'
    else:
        import_history.status = 'processing'
        import_history.phase = 'initializing'
    
    import_history.save()

    # Путь к загруженному файлу
    if not import_history.uploaded_file:
        import_history.status = 'failed'
        import_history.error_message = 'Не найден загруженный файл для импорта'
        import_history.save()
        return

    file_path = Path(import_history.uploaded_file.path)
    delimiter = import_history.delimiter
    encoding = import_history.encoding or 'utf-8'
    has_header = import_history.has_header

    # Подсчитываем общее количество записей один раз
    if not import_history.records_count:
        try:
            import_history.phase = 'counting'
            import_history.save()
            total = _count_total_records(file_path, delimiter, has_header)
            import_history.records_count = total
            import_history.progress_percent = 0
            import_history.save()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Не удалось подсчитать количество записей: {e}")

    # Архивируем один раз
    if not getattr(import_history, 'archived_done', False):
        try:
            import_history.phase = 'archiving'
            import_history.save()
            archive_table_name = f"subscribers_subscriber_archive_{int(timezone.now().timestamp())}"
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    CREATE TABLE {archive_table_name} AS 
                    SELECT * FROM subscribers_subscriber
                """)
                cursor.execute("DELETE FROM subscribers_subscriber")
            import_history.archive_table_name = archive_table_name
            import_history.archived_done = True
            import_history.save()
        except Exception as e:  # noqa: BLE001
            import_history.status = 'failed'
            import_history.error_message = f"Ошибка при архивации данных: {str(e)}"
            import_history.save()
            return

    id_pattern = re.compile(r'^\s*\d+')
    processed_rows_start = import_history.processed_rows or 0

    created_count = import_history.records_created or 0
    failed_count = import_history.records_failed or 0
    errors: list[str] = []

    # Используем новую логику с умным склеиванием строк
    try:
        created_count, failed_count, logical_row_index = _process_csv_lines_with_smart_joining(
            file_path, delimiter, encoding, has_header, import_history, processed_rows_start
        )
        
        # Обновляем финальную статистику
        import_history.processed_rows = logical_row_index
        import_history.records_created = created_count
        import_history.records_failed = failed_count
        import_history.status = 'completed'
        import_history.phase = 'completed'
        import_history.progress_percent = 100
        import_history.save()
    except Exception as e:
        import_history.status = 'failed'
        import_history.error_message = f"Непредвиденная ошибка: {str(e)}"
        import_history.save()
    finally:
        _RUNNING_IMPORTS.pop(import_history_id, None)

def start_import_async(import_history_id: int) -> bool:
    """Стартует фоновый импорт, если он ещё не идёт. Возвращает True, если стартовали сейчас."""
    if _RUNNING_IMPORTS.get(import_history_id):
        logger.info(f"Импорт {import_history_id} уже запущен, не запускаем повторно")
        return False
    
    logger.info(f"Запускаем фоновый импорт {import_history_id}")
    t = threading.Thread(target=process_csv_import_stream, args=(import_history_id,), daemon=True)
    _RUNNING_IMPORTS[import_history_id] = t
    t.start()
    return True

def is_import_running(import_history_id: int) -> bool:
    t = _RUNNING_IMPORTS.get(import_history_id)
    is_running = t.is_alive() if t else False
    return is_running

# Имитация задачи Celery для очистки устаревших данных
def cleanup_old_import_data(days=30):
    """
    Функция для очистки устаревших данных импорта
    
    Args:
        days (int): Количество дней для хранения данных
        
    Returns:
        str: Результат операции
    """
    try:
        return cleanup_old_import_data_impl(days)
    except Exception as e:
        logger.error(f"Ошибка при очистке устаревших данных: {str(e)}")
        return f"Ошибка при очистке устаревших данных: {str(e)}"

def cleanup_old_import_data_impl(days=30):
    """Реализация задачи очистки устаревших данных"""
    from django.utils import timezone
    from datetime import timedelta
    
    cutoff_date = timezone.now() - timedelta(days=days)
    
    try:
        # Удаляем старые записи импорта
        old_imports = ImportHistory.objects.filter(created_at__lt=cutoff_date)
        count = old_imports.count()
        old_imports.delete()
            
        # Удаляем старые ошибки импорта
        old_errors = ImportError.objects.filter(created_at__lt=cutoff_date)
        error_count = old_errors.count()
        old_errors.delete()
        
        logger.info(f"Удалено {count} старых записей импорта и {error_count} ошибок")
        return f"Успешно очищено: {count} записей импорта, {error_count} ошибок"
    
    except Exception as e:
        logger.error(f"Ошибка при очистке устаревших данных: {str(e)}")
        return f"Ошибка при очистке устаревших данных: {str(e)}"

def cleanup_old_archive_tables(keep_count=3):
    """
    Очистка устаревших архивных таблиц, оставляя только последние keep_count таблиц.
    
    Args:
        keep_count (int): Количество последних таблиц для сохранения
        
    Returns:
        dict: Результат операции с подробностями
    """
    from django.db import connection
    
    try:
        with connection.cursor() as cursor:
            # Получаем список всех архивных таблиц
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name LIKE 'subscribers_subscriber_archive_%'
                ORDER BY table_name DESC
            """)
            archive_tables = [row[0] for row in cursor.fetchall()]
            
            if len(archive_tables) <= keep_count:
                return {
                    "success": True,
                    "total_kept": len(archive_tables),
                    "total_deleted": 0,
                    "message": f"Все {len(archive_tables)} архивных таблиц сохранены"
                }
            
            # Определяем таблицы для удаления
            tables_to_keep = archive_tables[:keep_count]
            tables_to_delete = archive_tables[keep_count:]
            
            # Удаляем устаревшие таблицы
            for table in tables_to_delete:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
            
            return {
                "success": True,
                "total_kept": len(tables_to_keep),
                "total_deleted": len(tables_to_delete),
                "message": f"Сохранено: {len(tables_to_keep)}, удалено: {len(tables_to_delete)}"
            }
            
    except Exception as e:
        print(f"Ошибка при очистке старых архивных таблиц: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

def cleanup_old_archive_tables_task(keep_count=3):
    """
    Имитация Celery задачи для очистки архивных таблиц.
    """
    # Определяем delay как имитацию Celery
    def delay(keep_count=3):
        return cleanup_old_archive_tables(keep_count)
    
    # Добавляем метод delay к функции
    cleanup_old_archive_tables_task.delay = delay
    
    # Выполняем реальную работу
    return cleanup_old_archive_tables(keep_count) 
