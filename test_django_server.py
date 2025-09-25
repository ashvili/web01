#!/usr/bin/env python
"""
Скрипт для тестирования Django сервера и логгирования ошибок.
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

def test_django_startup():
    """Тестирует запуск Django."""
    print("=== Тест запуска Django ===")
    
    try:
        django.setup()
        print("✓ Django успешно запущен")
        
        # Проверяем настройки
        from django.conf import settings
        print(f"✓ DEBUG: {settings.DEBUG}")
        print(f"✓ ALLOWED_HOSTS: {settings.ALLOWED_HOSTS}")
        print(f"✓ ERROR_LOG_FILE: {getattr(settings, 'ERROR_LOG_FILE', 'НЕ НАЙДЕН')}")
        
        return True
        
    except Exception as e:
        print(f"✗ Ошибка запуска Django: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_logging_configuration():
    """Тестирует конфигурацию логгирования."""
    print("\n=== Тест конфигурации логгирования ===")
    
    try:
        from django.conf import settings
        import logging
        
        # Проверяем конфигурацию
        logging_config = getattr(settings, 'LOGGING', {})
        
        if 'handlers' in logging_config:
            print("✓ Handlers найдены")
            if 'error_file' in logging_config['handlers']:
                print("✓ error_file handler найден")
            else:
                print("✗ error_file handler не найден")
        else:
            print("✗ Handlers не найдены")
            
        if 'loggers' in logging_config:
            print("✓ Loggers найдены")
            if 'django.request' in logging_config['loggers']:
                print("✓ django.request logger найден")
            else:
                print("✗ django.request logger не найден")
        else:
            print("✗ Loggers не найдены")
            
        return True
        
    except Exception as e:
        print(f"✗ Ошибка конфигурации логгирования: {e}")
        return False

def test_error_logging():
    """Тестирует логгирование ошибок."""
    print("\n=== Тест логгирования ошибок ===")
    
    try:
        import logging
        
        # Тестируем разные логгеры
        loggers_to_test = [
            'django.request',
            'django.server',
            'vl09_web',
        ]
        
        for logger_name in loggers_to_test:
            logger = logging.getLogger(logger_name)
            logger.error(f"Тестовое сообщение от {logger_name}", extra={
                'test': True,
                'logger': logger_name
            })
            print(f"✓ Сообщение отправлено через {logger_name}")
            
        return True
        
    except Exception as e:
        print(f"✗ Ошибка логгирования: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_file_creation():
    """Тестирует создание файла логов."""
    print("\n=== Тест создания файла логов ===")
    
    try:
        from django.conf import settings
        error_log_file = getattr(settings, 'ERROR_LOG_FILE', None)
        
        if not error_log_file:
            print("✗ ERROR_LOG_FILE не настроен")
            return False
            
        # Проверяем, что файл существует
        if os.path.exists(error_log_file):
            print(f"✓ Файл логов существует: {error_log_file}")
            
            # Проверяем размер файла
            size = os.path.getsize(error_log_file)
            print(f"✓ Размер файла: {size} байт")
            
            # Проверяем права на запись
            if os.access(error_log_file, os.W_OK):
                print("✓ Права на запись: OK")
            else:
                print("✗ Нет прав на запись")
                return False
                
        else:
            print(f"✗ Файл логов не найден: {error_log_file}")
            return False
            
        return True
        
    except Exception as e:
        print(f"✗ Ошибка проверки файла: {e}")
        return False

def main():
    """Основная функция тестирования."""
    print("Тестирование Django сервера и логгирования ошибок")
    print("=" * 60)
    
    # Тест 1: Запуск Django
    django_ok = test_django_startup()
    
    # Тест 2: Конфигурация логгирования
    config_ok = test_logging_configuration()
    
    # Тест 3: Создание файла логов
    file_ok = test_file_creation()
    
    # Тест 4: Логгирование ошибок
    logging_ok = test_error_logging()
    
    print("\n" + "=" * 60)
    print("РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ:")
    print(f"Django запуск: {'✓ OK' if django_ok else '✗ FAIL'}")
    print(f"Конфигурация логгирования: {'✓ OK' if config_ok else '✗ FAIL'}")
    print(f"Создание файла логов: {'✓ OK' if file_ok else '✗ FAIL'}")
    print(f"Логгирование ошибок: {'✓ OK' if logging_ok else '✗ FAIL'}")
    
    if all([django_ok, config_ok, file_ok, logging_ok]):
        print("\n🎉 Все тесты пройдены успешно!")
        print("Django сервер готов к запуску на Ubuntu.")
        return 0
    else:
        print("\n❌ Некоторые тесты не пройдены.")
        print("Проверьте ошибки выше и исправьте их перед запуском на Ubuntu.")
        return 1

if __name__ == '__main__':
    sys.exit(main())

