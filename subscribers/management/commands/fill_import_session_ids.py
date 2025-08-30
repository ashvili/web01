from django.core.management.base import BaseCommand
from django.utils import timezone
from subscribers.models import ImportHistory, ImportError
import uuid


class Command(BaseCommand):
    help = 'Заполняет существующие записи ImportHistory уникальными import_session_id'

    def handle(self, *args, **options):
        # Получаем все записи ImportHistory без import_session_id
        import_histories = ImportHistory.objects.filter(import_session_id='')
        
        if not import_histories.exists():
            self.stdout.write(
                self.style.SUCCESS('Все записи ImportHistory уже имеют import_session_id')
            )
            return
        
        self.stdout.write(f'Найдено {import_histories.count()} записей для обновления...')
        
        updated_count = 0
        for import_history in import_histories:
            # Генерируем уникальный ID на основе существующих данных
            timestamp = import_history.created_at.strftime('%Y%m%d_%H%M%S')
            user_id = import_history.created_by.id if import_history.created_by else 0
            filename = (import_history.file_name[:15] if import_history.file_name else 'unknown').replace(' ', '_')
            unique_id = str(uuid.uuid4())[:6]
            
            import_session_id = f"imp_{timestamp}_{user_id}_{filename}_{unique_id}"
            
            # Обновляем ImportHistory
            import_history.import_session_id = import_session_id
            import_history.save(update_fields=['import_session_id'])
            
            # Обновляем связанные ImportError
            ImportError.objects.filter(import_history=import_history).update(
                import_session_id=import_session_id
            )
            
            updated_count += 1
            self.stdout.write(f'Обновлена запись {import_history.id}: {import_session_id}')
        
        self.stdout.write(
            self.style.SUCCESS(f'Успешно обновлено {updated_count} записей')
        )
