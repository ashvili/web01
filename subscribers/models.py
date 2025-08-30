from django.db import models
from django.utils import timezone
from django.contrib.auth.models import User

# Create your models here.

class Subscriber(models.Model):
    """Модель для хранения данных об абонентах"""
    GENDER_CHOICES = [
        ('M', 'Мужской'),
        ('F', 'Женский'),
    ]

    # Поля из CSV-файла
    original_id = models.PositiveIntegerField('Оригинальный ID', null=True, blank=True)
    number = models.CharField('Номер', max_length=20, unique=True, default='')
    last_name = models.CharField('Фамилия', max_length=100)
    first_name = models.CharField('Имя', max_length=100)
    middle_name = models.CharField('Отчество', max_length=100, blank=True, null=True)
    address = models.TextField('Адрес', blank=True, null=True)
    memo1 = models.CharField('Memo1', max_length=255, blank=True, null=True)
    memo2 = models.CharField('Memo2', max_length=255, blank=True, null=True)
    birth_place = models.CharField('Место рождения', max_length=255, blank=True, null=True)
    birth_date = models.DateField('Дата рождения', null=True, blank=True)
    imsi = models.CharField('IMSI', max_length=50, blank=True, null=True)
    
    # Дополнительные поля
    gender = models.CharField('Пол', max_length=1, choices=GENDER_CHOICES, null=True, blank=True)
    email = models.EmailField('Электронная почта', blank=True, null=True)
    
    # Метаданные о подписчике
    is_active = models.BooleanField('Активный', default=True)
    created_at = models.DateTimeField('Дата создания', auto_now_add=True)
    updated_at = models.DateTimeField('Дата обновления', auto_now=True)
    
    # Связь с импортом
    import_history = models.ForeignKey('ImportHistory', on_delete=models.SET_NULL, 
                                       null=True, blank=True, verbose_name='История импорта')
    
    class Meta:
        verbose_name = 'Абонент'
        verbose_name_plural = 'Абоненты'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['number']),
            models.Index(fields=['last_name', 'first_name']),
            models.Index(fields=['imsi']),
        ]
    
    def __str__(self):
        return f"{self.last_name} {self.first_name} ({self.number})"

class ImportHistory(models.Model):
    """Модель для хранения истории импорта данных"""
    STATUS_CHOICES = (
        ('pending', 'В ожидании'),
        ('processing', 'В обработке'),
        ('paused', 'Пауза'),
        ('completed', 'Завершено'),
        ('failed', 'Ошибка'),
        ('cancelled', 'Отменено'),
    )
    
    file_name = models.CharField('Имя файла', max_length=255)
    file_size = models.PositiveIntegerField('Размер файла', default=0)
    delimiter = models.CharField('Разделитель', max_length=3, default=',')
    encoding = models.CharField('Кодировка', max_length=20, default='utf-8')
    has_header = models.BooleanField('Есть заголовок', default=True)
    status = models.CharField('Статус', max_length=20, choices=STATUS_CHOICES, default='pending')
    records_count = models.PositiveIntegerField('Всего записей', default=0)
    records_created = models.PositiveIntegerField('Создано записей', default=0)
    records_failed = models.PositiveIntegerField('Ошибочных записей', default=0)
    archive_table_name = models.CharField('Имя архивной таблицы', max_length=255, blank=True, null=True)
    error_message = models.TextField('Сообщение об ошибке', blank=True, null=True)
    info_message = models.TextField('Информационное сообщение', blank=True, null=True)
    uploaded_file = models.FileField('Файл импорта', upload_to='imports/%Y/%m/%d/', blank=True, null=True)
    processed_rows = models.PositiveIntegerField('Обработано записей', default=0)
    phase = models.CharField('Этап', max_length=20, default='pending')
    archived_done = models.BooleanField('Архивирование завершено', default=False)
    progress_percent = models.PositiveIntegerField('Прогресс, %', default=0)
    pause_requested = models.BooleanField('Пауза запрошена', default=False)
    cancel_requested = models.BooleanField('Отмена запрошена', default=False)
    last_heartbeat_at = models.DateTimeField('Последний heartbeat', null=True, blank=True)
    stop_reason = models.CharField('Причина остановки', max_length=255, null=True, blank=True)
    created_at = models.DateTimeField('Дата создания', auto_now_add=True)
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, related_name='imports')
    import_session_id = models.CharField('Уникальный ID сессии импорта', max_length=50, unique=True, default='')
    
    class Meta:
        verbose_name = 'История импорта'
        verbose_name_plural = 'История импорта'
        ordering = ['-created_at']
    
    def __str__(self):
        return f'Импорт {self.file_name} ({self.created_at.strftime("%d.%m.%Y %H:%M")})'


class ImportError(models.Model):
    """Детализация ошибок импорта по строкам."""
    import_history = models.ForeignKey(ImportHistory, on_delete=models.CASCADE, related_name='errors')
    import_session_id = models.CharField('ID сессии импорта', max_length=50, db_index=True, default='')
    row_index = models.PositiveIntegerField('Номер записи', default=0)
    message = models.TextField('Сообщение об ошибке')
    raw_data = models.TextField('Исходные данные строки', blank=True, null=True)
    created_at = models.DateTimeField('Дата создания', auto_now_add=True)

    class Meta:
        verbose_name = 'Ошибка импорта'
        verbose_name_plural = 'Ошибки импорта'
        indexes = [
            models.Index(fields=['import_history', 'row_index']),
            models.Index(fields=['import_session_id', 'row_index']),
            models.Index(fields=['created_at']),
        ]

    def __str__(self):
        return f'Ошибка в записи {self.row_index}: {self.message[:50]}'
