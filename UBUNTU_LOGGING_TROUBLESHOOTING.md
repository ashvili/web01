# Устранение проблем с логгированием ошибок на Ubuntu

## Возможные ошибки и их решения

### 1. Ошибка: "Permission denied" при записи в файл логов

**Симптомы:**
```
PermissionError: [Errno 13] Permission denied: '/path/to/logfiles/errors.log'
```

**Причины:**
- Недостаточные права доступа к директории `logfiles`
- Файл логов принадлежит другому пользователю
- SELinux блокирует запись (если включен)

**Решения:**
```bash
# Установить правильные права на директорию
sudo chmod 755 logfiles
sudo chown -R $USER:$USER logfiles

# Если используется systemd или другой сервис
sudo chown -R www-data:www-data logfiles  # для веб-сервера
sudo chmod 755 logfiles
```

### 2. Ошибка: "No such file or directory"

**Симптомы:**
```
FileNotFoundError: [Errno 2] No such file or directory: '/path/to/logfiles/errors.log'
```

**Причины:**
- Директория `logfiles` не существует
- Неправильный путь в `ERROR_LOG_FILE`

**Решения:**
```bash
# Создать директорию
mkdir -p logfiles
chmod 755 logfiles

# Проверить переменную окружения
echo $ERROR_LOG_FILE

# Установить переменную окружения
export ERROR_LOG_FILE="/absolute/path/to/logfiles/errors.log"
```

### 3. Ошибка кодировки: "UnicodeDecodeError"

**Симптомы:**
```
UnicodeDecodeError: 'utf-8' codec can't decode byte 0xd2 in position 252
```

**Причины:**
- Файл логов создан с другой кодировкой (cp1251, latin-1)
- Системная локаль не настроена на UTF-8

**Решения:**
```bash
# Установить UTF-8 локаль
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8

# Или для русской локали
export LANG=ru_RU.UTF-8
export LC_ALL=ru_RU.UTF-8

# Пересоздать файл логов с правильной кодировкой
rm logfiles/errors.log
touch logfiles/errors.log
```

### 4. Ошибка: "Disk space" или "No space left"

**Симптомы:**
```
OSError: [Errno 28] No space left on device
```

**Причины:**
- Заполнен диск
- Файл логов слишком большой

**Решения:**
```bash
# Проверить свободное место
df -h

# Очистить старые логи
find logfiles/ -name "*.log*" -mtime +30 -delete

# Настроить ротацию логов (уже настроена в settings.py)
# maxBytes: 10MB, backupCount: 5
```

### 5. Ошибка: "ModuleNotFoundError" при импорте logging

**Симптомы:**
```
ModuleNotFoundError: No module named 'logging'
```

**Причины:**
- Неправильная виртуальная среда
- Python установлен неправильно

**Решения:**
```bash
# Активировать виртуальную среду
source venv/bin/activate

# Или создать новую
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 6. Ошибка: "Django settings not configured"

**Симптомы:**
```
django.core.exceptions.ImproperlyConfigured: Requested setting ERROR_LOG_FILE, but settings are not configured.
```

**Причины:**
- Django не инициализирован
- Неправильный DJANGO_SETTINGS_MODULE

**Решения:**
```bash
# Установить переменную окружения
export DJANGO_SETTINGS_MODULE=vl09_web.settings

# Или запустить через manage.py
python manage.py shell
```

## Диагностика проблем

### 1. Запуск диагностического скрипта
```bash
chmod +x ubuntu_logging_test.sh
./ubuntu_logging_test.sh
```

### 2. Проверка прав доступа
```bash
ls -la logfiles/
ls -la logfiles/errors.log
```

### 3. Проверка переменных окружения
```bash
env | grep -E "(ERROR_LOG_FILE|DJANGO_SETTINGS_MODULE|LANG)"
```

### 4. Тестирование записи в файл
```bash
echo "test" >> logfiles/errors.log
cat logfiles/errors.log
```

## Рекомендации для продакшена

### 1. Настройка systemd сервиса
```ini
[Unit]
Description=Django App
After=network.target

[Service]
Type=simple
User=www-data
Group=www-data
WorkingDirectory=/path/to/project
Environment=ERROR_LOG_FILE=/var/log/django/errors.log
Environment=DJANGO_SETTINGS_MODULE=vl09_web.settings
ExecStart=/path/to/venv/bin/python manage.py runserver
Restart=always

[Install]
WantedBy=multi-user.target
```

### 2. Настройка logrotate
```bash
# /etc/logrotate.d/django
/var/log/django/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 www-data www-data
    postrotate
        systemctl reload django-app
    endscript
}
```

### 3. Мониторинг логов
```bash
# Установка logwatch для мониторинга
sudo apt install logwatch

# Настройка для Django логов
sudo nano /etc/logwatch/conf/logfiles/django.conf
```

## Проверочный список

- [ ] Директория `logfiles` существует и доступна для записи
- [ ] Переменная `ERROR_LOG_FILE` установлена
- [ ] Системная локаль настроена на UTF-8
- [ ] Достаточно места на диске
- [ ] Виртуальная среда активирована
- [ ] Django настройки корректны
- [ ] Права на файлы установлены правильно
- [ ] Ротация логов настроена

## Полезные команды

```bash
# Просмотр логов в реальном времени
tail -f logfiles/errors.log

# Поиск ошибок в логах
grep -i "error" logfiles/errors.log

# Статистика логов
wc -l logfiles/errors.log
du -h logfiles/

# Очистка старых логов
find logfiles/ -name "*.log*" -mtime +7 -delete
```

