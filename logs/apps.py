from django.apps import AppConfig


class LogsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'logs'
    verbose_name = 'Журнал действий пользователей'
    
    def ready(self):
        """
        Этот метод вызывается, когда приложение полностью загружено.
        Здесь можно безопасно импортировать модели и запускать код.
        """
        # Убедимся, что теги шаблонов зарегистрированы
        from django.template.library import import_library
        try:
            import_library("logs.templatetags.log_tags")
        except:
            pass
