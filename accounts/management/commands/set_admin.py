from django.core.management.base import BaseCommand
from django.contrib.auth.models import User

class Command(BaseCommand):
    help = 'Sets a user as admin'

    def handle(self, *args, **options):
        try:
            user = User.objects.get(username='root')
            user.profile.user_type = 0
            user.profile.save()
            user.profile.update_permissions()
            self.stdout.write(self.style.SUCCESS(f'Successfully set {user.username} as admin'))
        except User.DoesNotExist:
            self.stdout.write(self.style.ERROR('User "root" does not exist')) 