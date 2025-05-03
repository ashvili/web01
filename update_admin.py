import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'web01.settings')
django.setup()

from django.contrib.auth.models import User
from accounts.models import UserProfile

# Обновляем пользователя root
user = User.objects.get(username='root')
user.profile.user_type = 0  # Устанавливаем тип "Администратор"
user.profile.save()
user.profile.update_permissions()  # Обновляем права

print(f'Updated user {user.username} to Admin (user_type=0)')
print(f'Current permissions: import={user.profile.can_import_data}, export={user.profile.can_export_data}, logs={user.profile.can_view_logs}') 