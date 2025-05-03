from django.db import models
from django.contrib.auth.models import User, Group
from django.db.models.signals import post_save
from django.dispatch import receiver

# Create your models here.

class UserProfile(models.Model):
    """Расширение стандартной модели пользователя"""
    USER_TYPES = (
        (0, 'Администратор'),
        (1, 'Пользователь 1 уровня'),
        (2, 'Пользователь 2 уровня'),
    )
    
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile', verbose_name='Пользователь')
    department = models.CharField('Отдел', max_length=100, blank=True, null=True)
    position = models.CharField('Должность', max_length=100, blank=True, null=True)
    phone_number = models.CharField('Телефон', max_length=20, blank=True, null=True)
    user_type = models.IntegerField('Тип пользователя', choices=USER_TYPES, default=1)
    
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
    
    def is_admin(self):
        """Проверка, является ли пользователь администратором"""
        return self.user_type == 0
    
    def is_user1(self):
        """Проверка, является ли пользователь пользователем 1 уровня"""
        return self.user_type == 1
    
    def is_user2(self):
        """Проверка, является ли пользователь пользователем 2 уровня"""
        return self.user_type == 2
    
    def update_permissions(self):
        """Обновляет права пользователя на основе его типа"""
        # Удаляем пользователя из всех групп
        self.user.groups.clear()
        
        # Добавляем пользователя в соответствующую группу
        try:
            if self.user_type == 0:
                group = Group.objects.get(name='Администратор')
                self.user.groups.add(group)
                self.can_import_data = True
                self.can_export_data = True
                self.can_view_logs = True
            elif self.user_type == 1:
                group = Group.objects.get(name='Пользователь1')
                self.user.groups.add(group)
                self.can_import_data = False
                self.can_export_data = True
                self.can_view_logs = True
            elif self.user_type == 2:
                group = Group.objects.get(name='Пользователь2')
                self.user.groups.add(group)
                self.can_import_data = False
                self.can_export_data = True
                self.can_view_logs = False
        except Group.DoesNotExist:
            # Если группа не существует, просто продолжаем
            pass
        
        self.save()

# Автоматическое создание/обновление профиля при создании/обновлении пользователя
@receiver(post_save, sender=User)
def create_or_update_user_profile(sender, instance, created, **kwargs):
    """Создает или обновляет профиль пользователя"""
    if created:
        profile = UserProfile.objects.create(user=instance)
        # Устанавливаем права по умолчанию
        profile.update_permissions()
    else:
        instance.profile.save()

@receiver(post_save, sender=User)
def save_user_profile(sender, instance, **kwargs):
    instance.profile.save()
