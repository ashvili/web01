from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('subscribers', '0008_importhistory_progress_percent'),
    ]

    operations = [
        migrations.AddField(
            model_name='importhistory',
            name='pause_requested',
            field=models.BooleanField(default=False, verbose_name='Пауза запрошена'),
        ),
        migrations.AddField(
            model_name='importhistory',
            name='cancel_requested',
            field=models.BooleanField(default=False, verbose_name='Отмена запрошена'),
        ),
        migrations.AddField(
            model_name='importhistory',
            name='last_heartbeat_at',
            field=models.DateTimeField(blank=True, null=True, verbose_name='Последний heartbeat'),
        ),
        migrations.AddField(
            model_name='importhistory',
            name='stop_reason',
            field=models.CharField(blank=True, max_length=255, null=True, verbose_name='Причина остановки'),
        ),
        migrations.CreateModel(
            name='ImportError',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('row_index', models.PositiveIntegerField(default=0, verbose_name='Номер записи')),
                ('message', models.TextField(verbose_name='Сообщение об ошибке')),
                ('created_at', models.DateTimeField(auto_now_add=True, verbose_name='Дата создания')),
                ('import_history', models.ForeignKey(on_delete=models.deletion.CASCADE, related_name='errors', to='subscribers.importhistory')),
            ],
            options={
                'verbose_name': 'Ошибка импорта',
                'verbose_name_plural': 'Ошибки импорта',
            },
        ),
        migrations.AddIndex(
            model_name='importerror',
            index=models.Index(fields=['import_history', 'row_index'], name='subscribers_import_history_row_idx'),
        ),
        migrations.AddIndex(
            model_name='importerror',
            index=models.Index(fields=['created_at'], name='subscribers_importerror_created_idx'),
        ),
    ]


