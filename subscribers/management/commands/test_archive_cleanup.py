from django.core.management.base import BaseCommand
from subscribers.tasks import list_archive_tables, cleanup_old_archive_tables


class Command(BaseCommand):
    help = 'Тестирование очистки архивных таблиц'

    def add_arguments(self, parser):
        parser.add_argument(
            '--keep',
            type=int,
            default=3,
            help='Количество архивных таблиц для сохранения (по умолчанию: 3)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Показать, что будет удалено, без фактического удаления'
        )

    def handle(self, *args, **options):
        keep_count = options['keep']
        dry_run = options['dry_run']

        self.stdout.write(self.style.SUCCESS(f'🔍 Проверка архивных таблиц (сохранить: {keep_count})'))
        self.stdout.write('=' * 60)

        # Получаем список архивных таблиц до очистки
        before_info = list_archive_tables()
        
        if not before_info['success']:
            self.stdout.write(self.style.ERROR(f'❌ Ошибка при получении списка архивов: {before_info["error"]}'))
            return

        self.stdout.write(f'📋 Найдено архивных таблиц: {before_info["total_count"]}')
        
        if before_info['tables']:
            self.stdout.write('\n📊 Детали архивных таблиц:')
            for table in before_info['tables']:
                self.stdout.write(f'  📋 {table["name"]}')
                self.stdout.write(f'     Колонок: {table["columns"]}, Строк: {table["rows"]}')
        else:
            self.stdout.write('ℹ️ Архивные таблицы не найдены')
            return

        if before_info['total_count'] <= keep_count:
            self.stdout.write(self.style.WARNING(f'\n⚠️ Все {before_info["total_count"]} таблиц будут сохранены (лимит: {keep_count})'))
            return

        # Определяем, какие таблицы будут удалены
        tables_to_delete = before_info['tables'][keep_count:]
        tables_to_keep = before_info['tables'][:keep_count]

        self.stdout.write(f'\n💾 Таблицы для сохранения ({len(tables_to_keep)}):')
        for table in tables_to_keep:
            self.stdout.write(f'  ✅ {table["name"]}')

        self.stdout.write(f'\n🗑️ Таблицы для удаления ({len(tables_to_delete)}):')
        for table in tables_to_delete:
            self.stdout.write(f'  ❌ {table["name"]}')

        if dry_run:
            self.stdout.write(self.style.WARNING('\n🔍 РЕЖИМ ПРЕДВАРИТЕЛЬНОГО ПРОСМОТРА - ничего не удаляется'))
            return

        # Выполняем очистку
        self.stdout.write(f'\n🚀 Выполняем очистку...')
        result = cleanup_old_archive_tables(keep_count)

        if result['success']:
            self.stdout.write(self.style.SUCCESS(f'✅ Очистка завершена успешно!'))
            self.stdout.write(f'   Сохранено: {result["total_kept"]}')
            self.stdout.write(f'   Удалено: {result["total_deleted"]}')
            
            # Получаем информацию после очистки
            after_info = list_archive_tables()
            if after_info['success']:
                self.stdout.write(f'\n📊 Результат очистки:')
                self.stdout.write(f'   До очистки: {before_info["total_count"]} таблиц')
                self.stdout.write(f'   После очистки: {after_info["total_count"]} таблиц')
                
                if after_info['tables']:
                    self.stdout.write(f'\n📋 Оставшиеся таблицы:')
                    for table in after_info['tables']:
                        self.stdout.write(f'  📋 {table["name"]}')
        else:
            self.stdout.write(self.style.ERROR(f'❌ Ошибка при очистке: {result["error"]}'))
