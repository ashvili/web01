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
        ('completed', 'Завершено'),
        ('failed', 'Ошибка'),
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
    created_at = models.DateTimeField('Дата создания', auto_now_add=True)
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, related_name='imports')
    
    class Meta:
        verbose_name = 'История импорта'
        verbose_name_plural = 'История импорта'
        ordering = ['-created_at']
    
    def __str__(self):
        return f'Импорт {self.file_name} ({self.created_at.strftime("%d.%m.%Y %H:%M")})'
