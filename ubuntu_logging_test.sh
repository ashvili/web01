#!/bin/bash
# Скрипт для тестирования логгирования ошибок на Ubuntu
# Запуск: chmod +x ubuntu_logging_test.sh && ./ubuntu_logging_test.sh

echo "=== Тест логгирования ошибок на Ubuntu ==="

# Проверяем, что мы в правильной директории
if [ ! -f "manage.py" ]; then
    echo "❌ Ошибка: manage.py не найден. Запустите скрипт из корня Django проекта."
    exit 1
fi

# Проверяем права на директорию logfiles
echo "Проверка прав доступа к директории logfiles..."
if [ ! -d "logfiles" ]; then
    echo "Создание директории logfiles..."
    mkdir -p logfiles
fi

# Устанавливаем правильные права
chmod 755 logfiles
echo "✓ Директория logfiles готова"

# Проверяем права на файл логов
if [ -f "logfiles/errors.log" ]; then
    chmod 644 logfiles/errors.log
    echo "✓ Файл errors.log готов"
else
    touch logfiles/errors.log
    chmod 644 logfiles/errors.log
    echo "✓ Файл errors.log создан"
fi

# Проверяем переменные окружения
echo "Проверка переменных окружения..."
if [ -z "$ERROR_LOG_FILE" ]; then
    echo "⚠️  ERROR_LOG_FILE не установлена, будет использован путь по умолчанию"
else
    echo "✓ ERROR_LOG_FILE установлена: $ERROR_LOG_FILE"
fi

# Проверяем кодировку системы
echo "Проверка кодировки системы..."
echo "LANG: $LANG"
echo "LC_ALL: $LC_ALL"

# Запускаем Python тест
echo "Запуск Python теста..."
python3 test_logging.py

# Проверяем содержимое файла логов
echo "Проверка содержимого файла логов..."
if [ -f "logfiles/errors.log" ]; then
    echo "Последние 5 строк файла логов:"
    tail -5 logfiles/errors.log
    echo "Размер файла: $(wc -c < logfiles/errors.log) байт"
else
    echo "❌ Файл логов не найден"
fi

echo "=== Тест завершен ==="

