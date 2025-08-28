from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('subscribers', '0006_importhistory_info_message'),
    ]

    operations = [
        migrations.AddField(
            model_name='importhistory',
            name='uploaded_file',
            field=models.FileField(blank=True, null=True, upload_to='imports/%Y/%m/%d/', verbose_name='Файл импорта'),
        ),
        migrations.AddField(
            model_name='importhistory',
            name='processed_rows',
            field=models.PositiveIntegerField(default=0, verbose_name='Обработано записей'),
        ),
        migrations.AddField(
            model_name='importhistory',
            name='phase',
            field=models.CharField(default='pending', max_length=20, verbose_name='Этап'),
        ),
        migrations.AddField(
            model_name='importhistory',
            name='archived_done',
            field=models.BooleanField(default=False, verbose_name='Архивирование завершено'),
        ),
    ]


