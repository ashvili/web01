from django.db import models
from django.contrib.auth.models import User
from django.db.models.signals import post_save
from django.dispatch import receiver

# Create your models here.

class UserProfile(models.Model):
    """Расширение стандартной модели пользователя"""
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile', verbose_name='Пользователь')
    department = models.CharField('Отдел', max_length=100, blank=True, null=True)
    position = models.CharField('Должность', max_length=100, blank=True, null=True)
    phone_number = models.CharField('Телефон', max_length=20, blank=True, null=True)
    
    # Настройки пользовательского интерфейса
    items_per_page = models.PositiveIntegerField('Записей на странице', default=25)
    theme = models.CharField('Тема оформления', max_length=20, default='light')
    
    # Двухфакторная аутентификация
    totp_secret = models.CharField('Секрет TOTP', max_length=100, blank=True, null=True)
    totp_enabled = models.BooleanField('2FA активирована', default=False)
    
    # Разрешения
    can_import_data = models.BooleanField('Может импортировать данные', default=False)
    can_export_data = models.BooleanField('Может экспортировать данные', default=False)
    can_view_logs = models.BooleanField('Может просматривать логи', default=False)
    
    class Meta:
        verbose_name = 'Профиль пользователя'
        verbose_name_plural = 'Профили пользователей'
    
    def __str__(self):
        return f"Профиль {self.user.username}"

# Автоматическое создание/обновление профиля при создании/обновлении пользователя
@receiver(post_save, sender=User)
def create_or_update_user_profile(sender, instance, created, **kwargs):
    """Создает или обновляет профиль пользователя"""
    if created:
        UserProfile.objects.create(user=instance)
    else:
        instance.profile.save()
