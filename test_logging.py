#!/usr/bin/env python
"""
Тестовый скрипт для проверки системы логгирования ошибок.
Помогает выявить проблемы при запуске на Ubuntu.
"""
import os
import sys
import django
from pathlib import Path

# Добавляем путь к проекту
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))

# Настройка Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'vl09_web.settings')

def test_logging_setup():
    """Тестирует настройку логгирования."""
    print("=== Тест настройки логгирования ===")
    
    try:
        # Инициализируем Django
        django.setup()
        print("✓ Django успешно инициализирован")
        
        # Проверяем настройки логгирования
        from django.conf import settings
        
        print(f"✓ ERROR_LOG_FILE: {getattr(settings, 'ERROR_LOG_FILE', 'НЕ НАЙДЕН')}")
        
        # Проверяем, что файл логов существует
        error_log_file = getattr(settings, 'ERROR_LOG_FILE', None)
        if error_log_file and os.path.exists(error_log_file):
            print(f"✓ Файл логов существует: {error_log_file}")
            
            # Проверяем права на запись
            try:
                with open(error_log_file, 'a', encoding='utf-8') as f:
                    f.write("Test write access\n")
                print("✓ Права на запись в файл логов: OK")
            except Exception as e:
                print(f"✗ Ошибка записи в файл логов: {e}")
        else:
            print(f"✗ Файл логов не найден: {error_log_file}")
            
        # Проверяем конфигурацию логгирования
        logging_config = getattr(settings, 'LOGGING', {})
        if 'handlers' in logging_config and 'error_file' in logging_config['handlers']:
            print("✓ Конфигурация error_file handler найдена")
        else:
            print("✗ Конфигурация error_file handler не найдена")
            
        return True
        
    except Exception as e:
        print(f"✗ Ошибка при инициализации Django: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_error_logging():
    """Тестирует логгирование ошибок."""
    print("\n=== Тест логгирования ошибок ===")
    
    try:
        import logging
        
        # Получаем логгер для ошибок
        error_logger = logging.getLogger('django.request')
        
        # Тестируем логгирование
        error_logger.error("Тестовое сообщение об ошибке", extra={
            'test': True,
            'test_module': 'test_logging'
        })
        print("✓ Тестовое сообщение об ошибке записано")
        
        # Проверяем, что сообщение записалось в файл
        from django.conf import settings
        error_log_file = getattr(settings, 'ERROR_LOG_FILE', None)
        
        if error_log_file and os.path.exists(error_log_file):
            # Пробуем разные кодировки
            encodings = ['utf-8', 'cp1251', 'latin-1', 'utf-16']
            content = None
            
            for encoding in encodings:
                try:
                    with open(error_log_file, 'r', encoding=encoding) as f:
                        content = f.read()
                    print(f"✓ Файл прочитан с кодировкой: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue
                    
            if content is None:
                print("✗ Не удалось прочитать файл ни с одной кодировкой")
                return False
                
            if "Тестовое сообщение об ошибке" in content:
                print("✓ Сообщение найдено в файле логов")
            else:
                print("✗ Сообщение не найдено в файле логов")
                print(f"Последние 300 символов файла:")
                print(content[-300:])
        
        return True
        
    except Exception as e:
        print(f"✗ Ошибка при тестировании логгирования: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_permissions():
    """Тестирует права доступа к файлам."""
    print("\n=== Тест прав доступа ===")
    
    try:
        from django.conf import settings
        error_log_file = getattr(settings, 'ERROR_LOG_FILE', None)
        
        if not error_log_file:
            print("✗ ERROR_LOG_FILE не настроен")
            return False
            
        # Проверяем директорию
        log_dir = os.path.dirname(error_log_file)
        if not os.path.exists(log_dir):
            print(f"✗ Директория не существует: {log_dir}")
            return False
            
        print(f"✓ Директория существует: {log_dir}")
        
        # Проверяем права на создание файла
        test_file = os.path.join(log_dir, 'test_permissions.tmp')
        try:
            with open(test_file, 'w', encoding='utf-8') as f:
                f.write("test")
            os.remove(test_file)
            print("✓ Права на создание файлов в директории: OK")
        except Exception as e:
            print(f"✗ Ошибка создания файла: {e}")
            return False
            
        # Проверяем права на запись в файл логов
        try:
            with open(error_log_file, 'a', encoding='utf-8') as f:
                f.write(f"Permission test at {os.getcwd()}\n")
            print("✓ Права на запись в файл логов: OK")
        except Exception as e:
            print(f"✗ Ошибка записи в файл логов: {e}")
            return False
            
        return True
        
    except Exception as e:
        print(f"✗ Ошибка при тестировании прав: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Основная функция тестирования."""
    print("Тестирование системы логгирования ошибок")
    print("=" * 50)
    
    # Тест 1: Настройка логгирования
    setup_ok = test_logging_setup()
    
    # Тест 2: Права доступа
    permissions_ok = test_permissions()
    
    # Тест 3: Логгирование ошибок
    logging_ok = test_error_logging()
    
    print("\n" + "=" * 50)
    print("РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ:")
    print(f"Настройка логгирования: {'✓ OK' if setup_ok else '✗ FAIL'}")
    print(f"Права доступа: {'✓ OK' if permissions_ok else '✗ FAIL'}")
    print(f"Логгирование ошибок: {'✓ OK' if logging_ok else '✗ FAIL'}")
    
    if all([setup_ok, permissions_ok, logging_ok]):
        print("\n🎉 Все тесты пройдены успешно!")
        return 0
    else:
        print("\n❌ Некоторые тесты не пройдены. Проверьте ошибки выше.")
        return 1

if __name__ == '__main__':
    sys.exit(main())
