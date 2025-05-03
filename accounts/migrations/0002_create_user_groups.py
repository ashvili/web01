from django.db import migrations
from django.contrib.auth.models import Group, Permission
from django.contrib.contenttypes.models import ContentType
from django.db.models import Q

def create_user_groups(apps, schema_editor):
    """
    Создаем группы пользователей и назначаем им права
    """
    # Создание групп, если они не существуют
    admin_group, created_admin = Group.objects.get_or_create(name='Администратор')
    user1_group, created_user1 = Group.objects.get_or_create(name='Пользователь1')
    user2_group, created_user2 = Group.objects.get_or_create(name='Пользователь2')
    
    # Получаем все существующие разрешения
    permissions = Permission.objects.all()
    
    # Разрешения для импорта данных и просмотра истории импорта
    import_permissions = Permission.objects.filter(
        Q(codename__contains='import') | 
        Q(codename__contains='history') |
        Q(codename__contains='log')
    )
    
    # Права на управление IMSI (предположим, что они связаны с моделью абонентов)
    try:
        subscriber_content_type = ContentType.objects.get(app_label='subscribers', model='subscriber')
        imsi_permissions = Permission.objects.filter(
            content_type=subscriber_content_type,
            codename__contains='imsi'
        )
    except ContentType.DoesNotExist:
        imsi_permissions = Permission.objects.none()
    
    # Администратор получает все права
    admin_group.permissions.set(permissions)
    
    # Пользователь 1 уровня получает все права, кроме импорта и истории
    user1_permissions = permissions.exclude(pk__in=import_permissions)
    user1_group.permissions.set(user1_permissions)
    
    # Пользователь 2 уровня получает те же права, что и Пользователь 1, 
    # но без доступа к полю IMSI
    user2_permissions = user1_permissions.exclude(pk__in=imsi_permissions)
    user2_group.permissions.set(user2_permissions)

def reverse_user_groups(apps, schema_editor):
    """
    Удаляем созданные группы
    """
    Group.objects.filter(name__in=['Администратор', 'Пользователь1', 'Пользователь2']).delete()

class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0001_initial'),
    ]

    operations = [
        migrations.RunPython(create_user_groups, reverse_user_groups),
    ] 