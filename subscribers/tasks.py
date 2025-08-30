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

def _create_temp_table(temp_table_name):
    """Создает временную таблицу с той же структурой, что и основная таблица subscribers_subscriber"""
    logger.info(f"🏗️ Создание временной таблицы: {temp_table_name}")
    with connection.cursor() as cursor:
        # Создаем временную таблицу точно по структуре основной таблицы
        cursor.execute(f"""
            CREATE TABLE {temp_table_name} (
                id SERIAL PRIMARY KEY,
                original_id INTEGER,
                number VARCHAR(20),
                last_name VARCHAR(100),
                first_name VARCHAR(100),
                middle_name VARCHAR(100),
                address TEXT,
                memo1 VARCHAR(255),
                memo2 VARCHAR(255),
                birth_place VARCHAR(255),
                birth_date DATE,
                imsi VARCHAR(50),
                gender VARCHAR(1),
                email VARCHAR(254),
                is_active BOOLEAN,
                created_at TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE,
                import_history_id INTEGER
            )
        """)
    logger.info(f"✅ Временная таблица {temp_table_name} создана успешно")
    return temp_table_name

def _insert_into_temp_table(temp_table_name, record_data):
    """Вставляет запись во временную таблицу"""
    logger.debug(f"📥 Вставка записи ID={record_data['original_id']} в {temp_table_name}")
    with connection.cursor() as cursor:
        # Дополнительная защита - обрезаем все поля до максимальной длины
        safe_data = [
            record_data['original_id'],
            (record_data['number'] or '')[:20],  # Номер: максимум 20 символов
            (record_data['last_name'] or '')[:100],  # Фамилия: максимум 100 символов
            (record_data['first_name'] or '')[:100],  # Имя: максимум 100 символов
            (record_data['middle_name'] or '')[:100] if record_data['middle_name'] else None,  # Отчество: максимум 100 символов
            record_data['address'],  # TEXT поле - без ограничений
            (record_data['memo1'] or '')[:255] if record_data['memo1'] else None,  # Memo1: максимум 255 символов
            (record_data['memo2'] or '')[:255] if record_data['memo2'] else None,  # Memo2: максимум 255 символов
            (record_data['birth_place'] or '')[:255] if record_data['birth_place'] else None,  # Место рождения: максимум 255 символов
            record_data['birth_date'],
            (record_data['imsi'] or '')[:50] if record_data['imsi'] else None,  # IMSI: максимум 50 символов
            None,  # gender
            None,  # email
            True,  # is_active
            timezone.now(),  # created_at
            timezone.now(),  # updated_at
            record_data['import_history_id']
        ]
        
        cursor.execute(f"""
            INSERT INTO {temp_table_name} (
                original_id, number, last_name, first_name, middle_name, 
                address, memo1, memo2, birth_place, birth_date, imsi, 
                gender, email, is_active, created_at, updated_at, import_history_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, safe_data)
    logger.debug(f"✅ Запись ID={record_data['original_id']} вставлена в {temp_table_name}")

def _finalize_import(import_history):
    """Финализирует импорт: архивирует основную таблицу и заменяет ее данными из временной"""
    temp_table_name = import_history.temp_table_name
    archive_table_name = f"subscribers_subscriber_archive_{int(timezone.now().timestamp())}"
    
    logger.info(f"🏁 Начинаем финализацию импорта...")
    logger.info(f"📁 Временная таблица: {temp_table_name}")
    logger.info(f"📦 Архивная таблица: {archive_table_name}")
    
    with connection.cursor() as cursor:
        try:
            # 1. Создаем архивную копию основной таблицы
            logger.info("📋 Создание архивной копии основной таблицы...")
            cursor.execute(f"""
                CREATE TABLE {archive_table_name} AS 
                SELECT * FROM subscribers_subscriber
            """)
            logger.info("✅ Архивная копия создана")
            
            # 2. Очищаем основную таблицу
            logger.info("🗑️ Очистка основной таблицы...")
            cursor.execute("DELETE FROM subscribers_subscriber")
            logger.info("✅ Основная таблица очищена")
            
            # 3. Копируем данные из временной таблицы в основную (без поля id - оно будет сгенерировано автоматически)
            logger.info("📤 Копирование данных из временной таблицы в основную...")
            cursor.execute(f"""
                INSERT INTO subscribers_subscriber (
                    original_id, number, last_name, first_name, middle_name,
                    address, memo1, memo2, birth_place, birth_date, imsi,
                    gender, email, is_active, created_at, updated_at, import_history_id
                )
                SELECT 
                    original_id, number, last_name, first_name, middle_name,
                    address, memo1, memo2, birth_place, birth_date, imsi,
                    gender, email, is_active, created_at, updated_at, import_history_id
                FROM {temp_table_name}
            """)
            logger.info("✅ Данные скопированы в основную таблицу")
            
            # 4. Удаляем временную таблицу
            logger.info("🗑️ Удаление временной таблицы...")
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            logger.info("✅ Временная таблица удалена")
            
            # 5. Обновляем ImportHistory
            import_history.archive_table_name = archive_table_name
            import_history.temp_table_name = None
            import_history.save()
            
            logger.info("🎉 Финализация импорта завершена успешно!")
            return True
        except Exception as e:
            # В случае ошибки оставляем все как есть
            logger.error(f"❌ Ошибка при финализации импорта: {str(e)}")
            raise Exception(f"Ошибка при финализации импорта: {str(e)}")

def _cleanup_temp_table(temp_table_name):
    """Удаляет временную таблицу при ошибке или отмене импорта"""
    if temp_table_name:
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        except Exception as e:
            logger.warning(f"Не удалось удалить временную таблицу {temp_table_name}: {str(e)}")

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
    """
    Подсчёт числа логических записей в CSV с учетом умного склеивания.
    Использует ту же логику, что и _process_csv_lines_with_smart_joining.
    """
    logger.info(f"📊 Подсчет общего количества записей с умным склеиванием...")
    
    total = 0
    
    with file_path.open('r', encoding='utf-8', errors='ignore') as fh:
        # Читаем все строки сразу для возможности предпросмотра
        all_lines = [line.rstrip('\n\r') for line in fh.readlines()]
        
        logger.info(f"📁 Файл прочитан для подсчета: {len(all_lines)} физических строк")
        
        i = 0
        while i < len(all_lines):
            current_line = _clean_line_for_combining(all_lines[i])
            physical_line_idx = i + 1  # Номер строки в файле (1-based)
            
            # Пропускаем заголовок
            if physical_line_idx == 1 and has_header:
                i += 1
                continue
                
            # Пропускаем пустые строки
            if not current_line:
                i += 1
                continue
            
            # Проверяем, является ли текущая строка валидной (ID + телефонный номер)
            is_current_valid = _is_valid_line(current_line, delimiter)
            
            if is_current_valid:
                # Текущая строка валидная - считаем как одну логическую запись
                total += 1
                
                # Пропускаем все следующие строки до следующей валидной строки
                j = i + 1
                while j < len(all_lines):
                    next_line = _clean_line_for_combining(all_lines[j])
                    
                    # Пропускаем пустые строки
                    if not next_line:
                        j += 1
                        continue
                    
                    is_next_valid = _is_valid_line(next_line, delimiter)
                    
                    if is_next_valid:
                        # Следующая строка валидная - прекращаем поиск
                        break
                    else:
                        # Следующая строка не валидная - пропускаем ее (это продолжение текущей записи)
                        j += 1
                
                # Переходим к найденной валидной строке или к концу файла
                i = j
            else:
                # Текущая строка не валидная - пропускаем (не должно быть при правильной логике)
                i += 1
    
    logger.info(f"📊 Подсчет завершен: {total} логических записей из {len(all_lines)} физических строк")
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

        logger.info(f"💾 Подготовка данных для записи: ID={parsed.get('original_id')}, номер={parsed.get('number')}")

        # Валидация длины полей перед вставкой
        validation_errors = []
        
        if parsed.get('number') and len(parsed['number']) > 20:
            validation_errors.append(f"Номер слишком длинный: {len(parsed['number'])} символов (максимум 20)")
            parsed['number'] = parsed['number'][:20]  # Обрезаем до максимальной длины
            
        if parsed.get('last_name') and len(parsed['last_name']) > 100:
            validation_errors.append(f"Фамилия слишком длинная: {len(parsed['last_name'])} символов (максимум 100)")
            parsed['last_name'] = parsed['last_name'][:100]
            
        if parsed.get('first_name') and len(parsed['first_name']) > 100:
            validation_errors.append(f"Имя слишком длинное: {len(parsed['first_name'])} символов (максимум 100)")
            parsed['first_name'] = parsed['first_name'][:100]
            
        if parsed.get('middle_name') and len(parsed['middle_name']) > 100:
            validation_errors.append(f"Отчество слишком длинное: {len(parsed['middle_name'])} символов (максимум 100)")
            parsed['middle_name'] = parsed['middle_name'][:100]
            
        if parsed.get('imsi') and len(parsed['imsi']) > 50:
            validation_errors.append(f"IMSI слишком длинный: {len(parsed['imsi'])} символов (максимум 50)")
            parsed['imsi'] = parsed['imsi'][:50]

        # Логируем предупреждения о валидации
        if validation_errors:
            logger.warning(f"⚠️ Предупреждения валидации для записи ID={parsed.get('original_id')}: {validation_errors}")

        # Подготавливаем данные для вставки во временную таблицу
        record_data = {
            'original_id': parsed['original_id'],
            'number': parsed['number'],
            'last_name': parsed['last_name'],
            'first_name': parsed['first_name'],
            'middle_name': parsed['middle_name'],
            'address': parsed['address'],
            'memo1': parsed['memo1'],
            'memo2': parsed['memo2'],
            'birth_place': parsed['birth_place'],
            'birth_date': parsed['birth_date'],
            'imsi': parsed['imsi'],
            'import_history_id': import_history.id,
        }
        
        logger.info(f"📝 Вставка во временную таблицу {import_history.temp_table_name}...")
        
        # Вставляем во временную таблицу
        _insert_into_temp_table(import_history.temp_table_name, record_data)
        created_count += 1
        
        logger.info(f"✅ Запись успешно сохранена во временную таблицу")
        
    except Exception as e:  # noqa: BLE001 - логируем и продолжаем
        failed_count += 1
        error_msg = f"Ошибка при создании записи: {str(e)}"
        errors.append(error_msg)
        logger.error(f"❌ Ошибка сохранения записи: {error_msg}")
        
        # Сохраняем исходные данные для анализа
        raw_data = f"ID: {parsed.get('original_id', 'N/A')}, Номер: {parsed.get('number', 'N/A')}, ФИО: {parsed.get('last_name', 'N/A')} {parsed.get('first_name', 'N/A')} {parsed.get('middle_name', 'N/A')}, Адрес: {parsed.get('address', 'N/A')}, Дата: {parsed.get('birth_date', 'N/A')}"
        
        # Проверяем размер raw_data
        raw_data_size = len(raw_data)
        if raw_data_size > 4000:
            logger.warning(f"⚠️ Большой размер raw_data в _process_record_row: {raw_data_size} символов")
        
        ImportError.objects.create(
            import_history=import_history,
            import_session_id=import_history.import_session_id,
            row_index=import_history.processed_rows + 1,
            message=error_msg,
            raw_data=raw_data[:5000]  # Увеличиваем лимит до 5000 символов
        )
    return created_count, failed_count, errors

def _clean_line_for_combining(line):
    """
    Очищает строку от лишних пробелов и непечатных символов.
    Убирает множественные пробелы, табуляции, переносы строк.
    Сохраняет структуру CSV (разделители, кавычки).
    """
    if not line:
        return ""
    
    # Заменяем табуляции и переносы строк на пробелы (но сохраняем разделители)
    cleaned = re.sub(r'[\t\r\n]+', ' ', line)
    
    # Убираем множественные пробелы, но сохраняем пробелы вокруг разделителей
    # Это важно для CSV, где пробелы могут быть частью данных
    cleaned = re.sub(r' +', ' ', cleaned)
    
    # Убираем пробелы в начале и конце строки
    cleaned = cleaned.strip()
    
    # Убираем лишние пробелы вокруг разделителей (но не внутри кавычек)
    # Это сложная операция, поэтому делаем базовую очистку
    cleaned = re.sub(r'\s*,\s*', ',', cleaned)  # Убираем пробелы вокруг запятых
    
    return cleaned

def _extract_id_from_line(line, delimiter):
    """Извлекает ID из первого поля строки."""
    if not line or not line.strip():
        return None
    
    try:
        first_field = line.split(delimiter)[0].strip()
        if not first_field:
            return None
        
        # Проверяем, что это целое число
        id_value = int(first_field)
        if id_value <= 0:
            return None
        
        return id_value
    except (ValueError, IndexError):
        return None

def _is_valid_line(line, delimiter):
    """Проверяет, является ли строка валидной (ID + телефонный номер)."""
    if not line or not line.strip():
        return False
    
    try:
        # Разбиваем строку по разделителю
        fields = line.split(delimiter)
        if len(fields) < 2:
            return False
        
        # Проверяем первое поле (ID)
        if not _is_valid_id_field(fields[0]):
            return False
        
        # Проверяем второе поле (телефонный номер)
        if not _is_valid_phone_field(fields[1]):
            return False
        
        return True
    except Exception:
        return False

def _is_valid_id_field_value(id_value):
    """Проверяет, является ли ID корректным значением."""
    if id_value is None or id_value <= 0:
        return False
    return True

def _is_valid_id_field(field_value):
    """Проверяет, является ли первое поле корректным ID."""
    if not field_value or not field_value.strip():
        return False
    
    try:
        parsed_id = int(field_value.strip())
        return _is_valid_id_field_value(parsed_id)
    except ValueError:
        return False

def _is_valid_phone_field(field_value):
    """Проверяет, является ли поле корректным телефонным номером."""
    if not field_value or not field_value.strip():
        return False
    
    # Убираем все пробелы, дефисы, скобки и другие символы
    phone = re.sub(r'[\s\-\(\)\+]', '', field_value.strip())
    
    # Проверяем, что остались только цифры
    if not phone.isdigit():
        return False
    
    # Проверяем длину (обычно 10-15 цифр)
    if len(phone) < 10 or len(phone) > 15:
        return False
    
    return True

def _is_valid_csv_line(row_values):
    """Проверяет, является ли строка CSV валидной."""
    if not row_values or len(row_values) < 2:
        return False
    
    # Проверяем первое поле (ID)
    if not _is_valid_id_field(row_values[0]):
        return False
    
    # Проверяем второе поле (телефонный номер)
    if not _is_valid_phone_field(row_values[1]):
        return False
    
    return True

def _try_parse_csv_line(line, delimiter):
    """Пробует распарсить строку как CSV и вернуть поля."""
    try:
        import csv
        import io
        # Очищаем строку от лишних пробелов перед парсингом
        cleaned_line = _clean_line_for_combining(line)
        csv_io = io.StringIO(cleaned_line)
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
    logger.info(f"🔍 Анализ объединенной строки для записи {logical_row_index}...")
    
    try:
        # Пытаемся распарсить объединенную строку
        logger.info(f"📝 Парсинг CSV: {combined_line[:200]}...")
        row_values = _try_parse_csv_line(combined_line, delimiter)
        if not row_values:
            logger.error(f"❌ Не удалось распарсить как CSV")
            return False, None
        
        logger.info(f"✅ CSV распарсен: {len(row_values)} полей")
        
        # Проверяем, что есть достаточно полей
        if len(row_values) < 8:
            logger.error(f"❌ Недостаточно полей: {len(row_values)} < 8")
            return False, None
        
        logger.info(f"✅ Количество полей OK: {len(row_values)}")
        
        # Получаем фактический ID
        actual_id = None
        if row_values[0] and row_values[0].strip():
            try:
                actual_id = int(row_values[0].strip())
                logger.info(f"✅ ID извлечен: {actual_id}")
            except ValueError:
                logger.error(f"❌ Не удалось преобразовать ID в число: '{row_values[0]}'")
                return False, None
        
        # Парсим запись
        logger.info(f"🔍 Парсинг полей записи...")
        parsed = _parse_line_to_record(row_values, logical_row_index, errors)
        if not parsed:
            logger.error(f"❌ Не удалось распарсить поля записи")
            return False, None
        
        logger.info(f"✅ Поля записи распарсены: {list(parsed.keys())}")
        
        # Пытаемся сохранить запись
        try:
            logger.info(f"💾 Сохранение записи во временную таблицу...")
            created_count, failed_count, errors = _process_record_row(parsed, import_history, (0, 0, errors))
            if failed_count == 0:
                logger.info(f"✅ Запись успешно сохранена во временную таблицу")
                return True, actual_id
            else:
                logger.error(f"❌ Ошибка при сохранении записи: {errors}")
                return False, actual_id
        except Exception as e:
            logger.error(f"❌ Исключение при сохранении записи: {str(e)}")
            return False, actual_id
            
    except Exception as e:
        logger.error(f"❌ Непредвиденная ошибка в _try_process_combined_line: {str(e)}")
        return False, None

def _parse_line_to_record(row_values, row_count, errors):
    """Преобразование массива строк в словарь полей."""
    try:
        if len(row_values) < 8:
            errors.append(f"Строка {row_count}: неверное количество полей ({len(row_values)})")
            return None
        original_id = None
        original_id_str = _clean_line_for_combining(row_values[0]) if row_values[0] else None
        if original_id_str:
            try:
                original_id = int(original_id_str)
            except ValueError:
                errors.append(f"Некорректный ID в строке {row_count}: {original_id_str}")
        number = _clean_line_for_combining(row_values[1]) if len(row_values) > 1 else ""
        last_name = _clean_line_for_combining(row_values[2]) if len(row_values) > 2 else ""
        first_name = _clean_line_for_combining(row_values[3]) if len(row_values) > 3 else ""
        middle_name = _clean_line_for_combining(row_values[4]) if len(row_values) > 4 else None
        address = _clean_line_for_combining(row_values[5]) if len(row_values) > 5 else None
        memo1 = _clean_line_for_combining(row_values[6]) if len(row_values) > 6 else None
        memo2 = _clean_line_for_combining(row_values[7]) if len(row_values) > 7 else None
        birth_place = _clean_line_for_combining(row_values[8]) if len(row_values) > 8 else None
        imsi = _clean_line_for_combining(row_values[10]) if len(row_values) > 10 else None

        # Дата рождения
        birth_date = None
        if len(row_values) > 9 and row_values[9] and _clean_line_for_combining(row_values[9]):
            from datetime import datetime, date
            birth_date_str = _clean_line_for_combining(row_values[9])
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
    
    last_valid_line = None  # Последняя строка с правильным полем
    
    with file_path.open('r', encoding=encoding, errors='ignore') as fh:
        import_history.phase = 'processing'
        import_history.save()
        
        # Читаем все строки сразу для возможности предпросмотра
        all_lines = [line.rstrip('\n\r') for line in fh.readlines()]
        
        logger.info(f"📁 Файл прочитан: {len(all_lines)} строк")
        logger.info(f"📊 Настройки: delimiter='{delimiter}', encoding='{encoding}', has_header={has_header}")
        logger.info(f"🚀 Начинаем обработку с позиции {processed_rows_start}")
        
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
                # Очищаем временную таблицу
                _cleanup_temp_table(import_history.temp_table_name)
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
                        # Очищаем временную таблицу
                        _cleanup_temp_table(import_history.temp_table_name)
                        return created_count, failed_count, logical_row_index
                    if not import_history.pause_requested:
                        logger.info(f"Импорт {import_history.id} возобновлен после паузы")
                        import_history.status = 'processing'
                        import_history.stop_reason = None
                        import_history.save()
                        break
                    import time
                    time.sleep(0.5)
            
            current_line = _clean_line_for_combining(all_lines[i])
            physical_line_idx = i + 1  # Номер строки в файле (1-based)
            
            # Пропускаем заголовок
            if physical_line_idx == 1 and has_header:
                i += 1
                continue
                
            # Пропускаем пустые строки
            if not current_line:
                i += 1
                continue
            
            # Проверяем, является ли текущая строка валидной (ID + телефонный номер)
            is_current_valid = _is_valid_line(current_line, delimiter)
            
            # ОТЛАДКА: Показываем текущую обрабатываемую строку
            logger.info(f"=== ОБРАБОТКА СТРОКИ {physical_line_idx} ===")
            logger.info(f"Текущая строка: {current_line[:200]}...")
            logger.info(f"Валидна: {is_current_valid}")
            
            if is_current_valid:
                # Текущая строка валидная - сохраняем как последнюю валидную
                last_valid_line = current_line
                logger.info(f"✅ Начинаем обработку валидной строки")
                
                # Смотрим следующие строки для склеивания
                combined_line = current_line
                lines_to_combine = [current_line]
                j = i + 1
                next_valid_line = None
                next_valid_line_index = None
                
                # Ищем следующую валидную строку или достигаем конца файла
                while j < len(all_lines):
                    next_line = _clean_line_for_combining(all_lines[j])
                    
                    # Пропускаем пустые строки
                    if not next_line:
                        j += 1
                        continue
                    
                    is_next_valid = _is_valid_line(next_line, delimiter)
                    
                    logger.info(f"  Следующая строка {j}: Валидна={is_next_valid}")
                    logger.info(f"  Содержимое: {next_line[:150]}...")
                    
                    if is_next_valid:
                        # Следующая строка валидная - прекращаем склеивание
                        next_valid_line = next_line
                        next_valid_line_index = j
                        logger.info(f"  🛑 Следующая строка валидна - прекращаем склеивание")
                        break
                    else:
                        # Следующая строка не валидная - добавляем к текущей
                        # Очищаем объединенную строку от лишних пробелов
                        combined_line = _clean_line_for_combining(combined_line + " " + next_line)
                        lines_to_combine.append(next_line)
                        logger.info(f"  🔗 Склеиваем строку {j}: {next_line[:100]}...")
                        logger.info(f"  📝 Объединенная строка: {combined_line[:200]}...")
                        j += 1
                
                logger.info(f"📊 Итоговое объединение: {len(lines_to_combine)} строк")
                logger.info(f"📝 Финальная строка: {combined_line[:300]}...")
                
                # Пытаемся обработать объединенную строку
                logical_row_index += 1
                if logical_row_index > processed_rows_start:
                    logger.info(f"🔄 Пытаемся обработать запись {logical_row_index}...")
                    
                    success, actual_id = _try_process_combined_line(
                        combined_line, logical_row_index, delimiter, import_history
                    )
                    
                    if success:
                        created_count += 1
                        logger.info(f"✅ Запись {logical_row_index} успешно обработана!")
                    else:
                        failed_count += 1
                        logger.error(f"❌ Запись {logical_row_index} не удалось обработать")
                        
                        # Записываем ошибку с подробными исходными данными
                        raw_data_lines = []
                        
                        # Записываем строки, начиная с последней валидной строки (если она отличается от текущей)
                        if last_valid_line and last_valid_line != current_line:
                            raw_data_lines.append(f"Последняя валидная строка: {last_valid_line}")
                        
                        # Записываем все строки, которые пытались склеить
                        for idx, line in enumerate(lines_to_combine):
                            if idx == 0:
                                raw_data_lines.append(f"Начальная строка (с валидным ID): {line}")
                            else:
                                raw_data_lines.append(f"Продолжение строки {idx}: {line}")
                        
                        # Добавляем следующую валидную строку, если она есть
                        if next_valid_line:
                            raw_data_lines.append(f"Следующая валидная строка: {next_valid_line}")
                        
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
                        
                        # Формируем финальный текст для raw_data
                        final_raw_data = "\n".join(raw_data_lines)
                        raw_data_size = len(final_raw_data)
                        
                        # Логируем размер данных для отладки
                        logger.info(f"📊 Размер raw_data для ошибки: {raw_data_size} символов")
                        if raw_data_size > 4000:
                            logger.warning(f"⚠️ Большой размер raw_data: {raw_data_size} символов (близко к лимиту 5000)")
                        
                        # Обрезаем до лимита, если необходимо
                        if raw_data_size > 5000:
                            final_raw_data = final_raw_data[:5000]
                            logger.warning(f"⚠️ raw_data обрезан с {raw_data_size} до 5000 символов")
                        
                        ImportError.objects.create(
                            import_history=import_history,
                            import_session_id=import_history.import_session_id,
                            row_index=logical_row_index,
                            message="Не удалось восстановить разбитую запись",
                            raw_data=final_raw_data
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
                
                # Переходим к следующей строке
                if next_valid_line_index is not None:
                    # У нас есть следующая валидная строка - переходим к ней
                    i = next_valid_line_index
                    logger.info(f"🔄 Переходим к валидной строке {i}: {next_valid_line[:100]}...")
                else:
                    # Достигли конца файла - переходим к j (конец файла)
                    i = j
                    logger.info(f"🔄 Достигли конца файла, переходим к позиции {i}")
                
                logger.info("=" * 80)
            else:
                # Текущая строка не валидная - пропускаем (такого не должно быть при правильной логике)
                logger.warning(f"⚠️ Строка {physical_line_idx} не валидна - пропускаем")
                i += 1
    
    logger.info(f"🏁 Обработка завершена!")
    logger.info(f"📊 Итоги: создано={created_count}, ошибок={failed_count}, обработано строк={logical_row_index}")
    logger.info("=" * 80)
    
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
                        raw_data=line[:5000]  # Увеличиваем лимит до 5000 символов
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
                        raw_data=line[:5000]  # Увеличиваем лимит до 5000 символов
                    )
    except Exception as e:
        failed_count += 1
        ImportError.objects.create(
            import_history=import_history,
            import_session_id=import_history.import_session_id,
            row_index=logical_row_index,
            message=f"Ошибка обработки строки: {str(e)}",
            raw_data=line[:5000]  # Увеличиваем лимит до 5000 символов
        )
    
    return created_count, failed_count, actual_id

def process_csv_import_stream(import_history_id: int) -> None:
    """Потоковый импорт с возможностью резюме по ImportHistory.processed_rows."""
    import_history = ImportHistory.objects.get(id=import_history_id)
    logger.info(f"🚀 Запуск потокового импорта {import_history_id}")
    logger.info(f"📊 Текущий статус: {import_history.status}")
    logger.info(f"📁 Файл: {import_history.uploaded_file}")
    
    # Если импорт был в паузе, продолжаем с того места, где остановились
    if import_history.status == 'paused':
        logger.info(f"⏸️ Возобновляем импорт {import_history_id} с позиции {import_history.processed_rows}")
        import_history.status = 'processing'
        import_history.phase = 'processing'
    else:
        logger.info(f"🆕 Новый импорт - инициализация...")
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

    # Создаем временную таблицу один раз
    if not import_history.temp_table_name:
        try:
            logger.info("🏗️ Создание временной таблицы для импорта...")
            import_history.phase = 'creating_temp_table'
            import_history.save()
            temp_table_name = f"subscribers_subscriber_temp_{int(timezone.now().timestamp())}"
            _create_temp_table(temp_table_name)
            import_history.temp_table_name = temp_table_name
            import_history.save()
            logger.info(f"✅ Временная таблица {temp_table_name} готова к использованию")
        except Exception as e:  # noqa: BLE001
            logger.error(f"❌ Ошибка при создании временной таблицы: {str(e)}")
            import_history.status = 'failed'
            import_history.error_message = f"Ошибка при создании временной таблицы: {str(e)}"
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
        
        # Завершение импорта во временную таблицу (без финализации)
        import_history.status = 'temp_completed'
        import_history.phase = 'waiting_finalization'  # Сокращаем до 18 символов
        import_history.progress_percent = 100
        if errors:
            msg = "\n".join(errors[:20])
            if len(errors) > 20:
                msg += f"\n... ещё {len(errors) - 20} ошибок"
            import_history.error_message = msg
        import_history.save()
        logger.info("🎉 Импорт во временную таблицу успешно завершен! Ожидаем команду на финализацию.")
    except Exception as e:
        logger.error(f"❌ Непредвиденная ошибка в процессе импорта: {str(e)}")
        import_history.status = 'failed'
        import_history.error_message = f"Непредвиденная ошибка: {str(e)}"
        import_history.save()
        # Очищаем временную таблицу при ошибке
        _cleanup_temp_table(import_history.temp_table_name)
    finally:
        # Убеждаемся, что временная таблица очищена при любом завершении
        if import_history.temp_table_name and import_history.status in ['failed', 'cancelled']:
            logger.info(f"🧹 Очистка временной таблицы {import_history.temp_table_name}")
            _cleanup_temp_table(import_history.temp_table_name)
        _RUNNING_IMPORTS.pop(import_history_id, None)
        logger.info(f"🏁 Импорт {import_history_id} завершен. Статус: {import_history.status}")

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
