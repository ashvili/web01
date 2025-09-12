from django.db import models
from django.utils import timezone
from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes.fields import GenericForeignKey
from django.utils.translation import gettext_lazy as _
from uuid import uuid4

# Create your models here.

class UserActionLog(models.Model):
    """Модель для логирования действий пользователей"""
    ACTION_TYPES = [
        ('LOGIN', _('Вход в систему')),
        ('LOGOUT', _('Выход из системы')),
        ('CREATE', _('Создание объекта')),
        ('UPDATE', _('Изменение объекта')),
        ('DELETE', _('Удаление объекта')),
        ('IMPORT', _('Импорт данных')),
        ('SEARCH', _('Поиск')),
        ('EXPORT', _('Экспорт данных')),
        ('OTHER', _('Другое')),
    ]
    
    user = models.ForeignKey(User, on_delete=models.CASCADE, verbose_name=_('Пользователь'))
    action_type = models.CharField(_('Тип действия'), max_length=20, choices=ACTION_TYPES)
    action_time = models.DateTimeField(_('Время действия'), default=timezone.now)
    ip_address = models.GenericIPAddressField(_('IP адрес'), null=True, blank=True)
    user_agent = models.TextField('User Agent', null=True, blank=True)
    # Расширенные поля для логирования
    path = models.CharField(_('Путь запроса'), max_length=255, default='/')
    method = models.CharField('HTTP метод', max_length=10, default='GET')
    status_code = models.PositiveIntegerField(_('HTTP код ответа'), null=True, blank=True)
    duration_ms = models.FloatField(_('Время обработки (ms)'), null=True, blank=True)
    
    # Для связи с любым объектом в системе (через ContentType framework)
    content_type = models.ForeignKey(ContentType, on_delete=models.SET_NULL, 
                                     null=True, blank=True, verbose_name='Тип объекта')
    object_id = models.PositiveIntegerField('ID объекта', null=True, blank=True)
    content_object = GenericForeignKey('content_type', 'object_id')
    
    # Дополнительные данные в формате JSON
    additional_data = models.JSONField('Дополнительные данные', null=True, blank=True)
    
    logical_session_id = models.UUIDField('Логическая сессия', null=True, blank=True, db_index=True, help_text='Группировка действий по логическим сессиям (gap-based)')
    
    related_log = models.ForeignKey('self', null=True, blank=True, on_delete=models.SET_NULL, db_index=True, help_text='Связанный лог (цепочка действий)')
    
    class Meta:
        verbose_name = _('Лог действий пользователя')
        verbose_name_plural = _('Логи действий пользователей')
        ordering = ['-action_time']
        indexes = [
            models.Index(fields=['user']),
            models.Index(fields=['action_type']),
            models.Index(fields=['action_time']),
            models.Index(fields=['content_type', 'object_id']),
        ]
    
    def __str__(self):
        return f"{self.get_action_type_display()} - {self.user.username} - {self.action_time.strftime('%d.%m.%Y %H:%M:%S')}"
