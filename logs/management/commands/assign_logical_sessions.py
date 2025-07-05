from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from logs.utils import assign_logical_sessions

class Command(BaseCommand):
    help = 'Группирует логи пользователей по логическим сессиям (gap-based) и присваивает logical_session_id.'

    def add_arguments(self, parser):
        parser.add_argument('--gap-hours', type=float, default=5, help='Интервал (в часах) для разрыва между сессиями (по умолчанию 5)')

    def handle(self, *args, **options):
        gap_hours = options['gap_hours']
        User = get_user_model()
        users = User.objects.all()
        for user in users:
            self.stdout.write(f'Обработка пользователя: {user.username}')
            assign_logical_sessions(user, gap_hours=gap_hours)
        self.stdout.write(self.style.SUCCESS('Логические сессии присвоены всем пользователям.')) 