from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('subscribers', '0007_importhistory_progress_fields'),
    ]

    operations = [
        migrations.AddField(
            model_name='importhistory',
            name='progress_percent',
            field=models.PositiveIntegerField(default=0, verbose_name='Прогресс, %'),
        ),
    ]


