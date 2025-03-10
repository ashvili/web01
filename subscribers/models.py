from django.db import models
from django.utils import timezone

# Create your models here.

class Subscriber(models.Model):
    """Модель для хранения данных об абонентах"""
    GENDER_CHOICES = [
        ('M', 'Мужской'),
        ('F', 'Женский'),
    ]

    first_name = models.CharField('Имя', max_length=100)
    last_name = models.CharField('Фамилия', max_length=100)
    middle_name = models.CharField('Отчество', max_length=100, blank=True, null=True)
    birth_date = models.DateField('Дата рождения', null=True, blank=True)
    gender = models.CharField('Пол', max_length=1, choices=GENDER_CHOICES, null=True, blank=True)
    phone_number = models.CharField('Номер телефона', max_length=20, unique=True)
    email = models.EmailField('Электронная почта', blank=True, null=True)
    address = models.TextField('Адрес', blank=True, null=True)
    
    # Метаданные о подписчике
    is_active = models.BooleanField('Активный', default=True)
    created_at = models.DateTimeField('Дата создания', auto_now_add=True)
    updated_at = models.DateTimeField('Дата обновления', auto_now=True)
    
    # Идентификатор импорта, связь с моделью ImportHistory
    import_id = models.PositiveIntegerField('Идентификатор импорта', null=True, blank=True)
    
    class Meta:
        verbose_name = 'Абонент'
        verbose_name_plural = 'Абоненты'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['phone_number']),
            models.Index(fields=['last_name', 'first_name']),
        ]
    
    def __str__(self):
        return f"{self.last_name} {self.first_name} ({self.phone_number})"

class ImportHistory(models.Model):
    """Модель для хранения истории импорта данных"""
    file_name = models.CharField('Имя файла', max_length=255)
    file_size = models.PositiveIntegerField('Размер файла в байтах')
    records_count = models.PositiveIntegerField('Количество импортированных записей')
    created_at = models.DateTimeField('Дата импорта', default=timezone.now)
    created_by = models.ForeignKey('auth.User', on_delete=models.SET_NULL, null=True, 
                                  verbose_name='Создано пользователем')
    
    class Meta:
        verbose_name = 'История импорта'
        verbose_name_plural = 'История импортов'
        ordering = ['-created_at']
    
    def __str__(self):
        return f"Импорт {self.file_name} ({self.records_count} записей) от {self.created_at.strftime('%d.%m.%Y %H:%M')}"
