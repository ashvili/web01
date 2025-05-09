from django.db import models
from django.utils import timezone
from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes.fields import GenericForeignKey

# Create your models here.

class UserActionLog(models.Model):
    """Модель для логирования действий пользователей"""
    ACTION_TYPES = [
        ('LOGIN', 'Вход в систему'),
        ('LOGOUT', 'Выход из системы'),
        ('CREATE', 'Создание объекта'),
        ('UPDATE', 'Изменение объекта'),
        ('DELETE', 'Удаление объекта'),
        ('IMPORT', 'Импорт данных'),
        ('SEARCH', 'Поиск'),
        ('EXPORT', 'Экспорт данных'),
        ('OTHER', 'Другое'),
    ]
    
    user = models.ForeignKey(User, on_delete=models.CASCADE, verbose_name='Пользователь')
    action_type = models.CharField('Тип действия', max_length=20, choices=ACTION_TYPES)
    action_time = models.DateTimeField('Время действия', default=timezone.now)
    ip_address = models.GenericIPAddressField('IP адрес', null=True, blank=True)
    user_agent = models.TextField('User Agent', null=True, blank=True)
    # Расширенные поля для логирования
    path = models.CharField('Путь запроса', max_length=255, default='/')
    method = models.CharField('HTTP метод', max_length=10, default='GET')
    status_code = models.PositiveIntegerField('HTTP код ответа', null=True, blank=True)
    duration_ms = models.FloatField('Время обработки (ms)', null=True, blank=True)
    
    # Для связи с любым объектом в системе (через ContentType framework)
    content_type = models.ForeignKey(ContentType, on_delete=models.SET_NULL, 
                                     null=True, blank=True, verbose_name='Тип объекта')
    object_id = models.PositiveIntegerField('ID объекта', null=True, blank=True)
    content_object = GenericForeignKey('content_type', 'object_id')
    
    # Дополнительные данные в формате JSON
    additional_data = models.JSONField('Дополнительные данные', null=True, blank=True)
    
    class Meta:
        verbose_name = 'Лог действий пользователя'
        verbose_name_plural = 'Логи действий пользователей'
        ordering = ['-action_time']
        indexes = [
            models.Index(fields=['user']),
            models.Index(fields=['action_type']),
            models.Index(fields=['action_time']),
            models.Index(fields=['content_type', 'object_id']),
        ]
    
    def __str__(self):
        return f"{self.get_action_type_display()} - {self.user.username} - {self.action_time.strftime('%d.%m.%Y %H:%M:%S')}"
