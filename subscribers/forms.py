from django import forms
from django.utils.translation import gettext_lazy as _

class CSVImportForm(forms.Form):
    """Объединенная форма для импорта данных из CSV-файла"""
    csv_file = forms.FileField(
        label=_('CSV-файл'),
        help_text=_('Выберите CSV-файл с данными абонентов'),
        widget=forms.FileInput(attrs={'class': 'form-control', 'accept': '.csv'})
    )
    
    delimiter_choices = [
        (',', _('Запятая (,)')),
        (';', _('Точка с запятой (;)')),
        ('\t', _('Табуляция (Tab)')),
        ('|', _('Вертикальная черта (|)')),
        (' ', _('Пробел')),
    ]
    
    encoding_choices = [
        ('utf-8', 'UTF-8'),
        ('cp1251', _('Windows-1251 (кириллица)')),
        ('latin1', 'Latin-1 (ISO-8859-1)'),
        ('ascii', 'ASCII'),
    ]
    
    delimiter = forms.ChoiceField(
        label=_('Разделитель полей'),
        choices=delimiter_choices,
        initial=',',
        help_text=_('Выберите символ, которым разделены поля в CSV-файле'),
        widget=forms.Select(attrs={'class': 'form-control'})
    )
    
    encoding = forms.ChoiceField(
        label=_('Кодировка файла'),
        choices=encoding_choices,
        initial='utf-8',
        help_text=_('Выберите кодировку файла'),
        widget=forms.Select(attrs={'class': 'form-control'})
    )
    
    has_header = forms.BooleanField(
        label=_('Первая строка содержит заголовки'),
        initial=True,
        required=False,
        help_text=_('Отметьте, если первая строка файла содержит названия колонок'),
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'})
    )
    

class SearchForm(forms.Form):
    """Форма для поиска абонентов"""
    phone_number = forms.CharField(
        label=_('Номер телефона'),
        required=False,
        widget=forms.TextInput(
            attrs={
                'class': 'form-control',
                'placeholder': _('Введите номер телефона'),
                'pattern': '[0-9]*',
                'title': _('Можно вводить только цифры'),
                'oninput': 'this.value = this.value.replace(/[^0-9]/g, "")'
            }
        )
    )
    
    full_name = forms.CharField(
        label=_('ФИО'),
        required=False,
        widget=forms.TextInput(
            attrs={
                'class': 'form-control',
                'placeholder': _('Введите имя, фамилию или отчество'),
            }
        )
    )
    
    passport = forms.CharField(
        label=_('Номер паспорта'),
        required=False,
        widget=forms.TextInput(
            attrs={
                'class': 'form-control',
                'placeholder': _('Введите номер паспорта'),
            }
        ),
        help_text=_('Номер паспорта хранится в поле Memo1')
    )
    
    address = forms.CharField(
        label=_('Адрес'),
        required=False,
        widget=forms.TextInput(
            attrs={
                'class': 'form-control',
                'placeholder': _('Введите адрес'),
            }
        )
    )
    
    def clean_phone_number(self):
        """Валидация номера телефона"""
        phone_number = self.cleaned_data.get('phone_number')
        if phone_number:
            # Удаляем все нецифровые символы
            phone_number = ''.join(filter(str.isdigit, phone_number))
        return phone_number 