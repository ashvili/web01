from django import template
import json

register = template.Library()

@register.filter(name='pprint')
def pprint_filter(value):
    """
    Форматирует JSON-объект для красивого отображения в шаблоне.
    
    Пример использования:
    {{ log.additional_data|pprint }}
    """
    try:
        if isinstance(value, dict):
            formatted = json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True)
        else:
            formatted = str(value)
        return formatted
    except Exception as e:
        return f"Ошибка форматирования: {str(e)}"

@register.filter(name='get_item')
def get_item_filter(dictionary, key):
    """
    Получает значение элемента словаря по ключу.
    
    Пример использования:
    {{ log.additional_data|get_item:'key' }}
    """
    if not dictionary:
        return None
    return dictionary.get(key, None)

@register.filter(name='action_type_badge_class')
def action_type_badge_class_filter(action_type):
    """
    Возвращает класс CSS для значка (badge) в зависимости от типа действия.
    
    Пример использования:
    <span class="badge {{ log.action_type|action_type_badge_class }}">{{ log.get_action_type_display }}</span>
    """
    classes = {
        'LOGIN': 'bg-success',
        'LOGOUT': 'bg-info',
        'CREATE': 'bg-primary',
        'UPDATE': 'bg-warning',
        'DELETE': 'bg-danger',
        'IMPORT': 'bg-primary',
        'EXPORT': 'bg-info',
        'SEARCH': 'bg-secondary',
        'OTHER': 'bg-secondary',
    }
    return classes.get(action_type, 'bg-secondary') 