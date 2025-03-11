from django import forms

class CSVImportForm(forms.Form):
    """Форма для загрузки CSV-файла"""
    csv_file = forms.FileField(
        label='CSV-файл',
        help_text='Выберите CSV-файл с данными абонентов',
        widget=forms.FileInput(attrs={'class': 'form-control', 'accept': '.csv'})
    )
    
    delimiter = forms.ChoiceField(
        label='Разделитель',
        choices=[
            (',', 'Запятая (,)'),
            (';', 'Точка с запятой (;)'),
            ('tab', 'Табуляция'),
        ],
        initial=',',
        widget=forms.Select(attrs={'class': 'form-control'})
    )
    
    encoding = forms.ChoiceField(
        label='Кодировка',
        choices=[
            ('utf-8', 'UTF-8'),
            ('cp1251', 'Windows-1251'),
            ('latin1', 'Latin-1'),
        ],
        initial='utf-8',
        widget=forms.Select(attrs={'class': 'form-control'})
    )
    
    has_header = forms.BooleanField(
        label='Содержит заголовок',
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'})
    )
    
    update_existing = forms.BooleanField(
        label='Обновлять существующие записи',
        required=False,
        initial=True,
        help_text='Если отмечено, существующие записи будут обновлены. В противном случае будут созданы дубликаты.',
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'})
    )

class ImportCSVForm(forms.Form):
    """Форма для импорта данных из CSV-файла"""
    csv_file = forms.FileField(
        label='Файл CSV',
        help_text='Выберите CSV-файл для импорта абонентов',
        widget=forms.FileInput(attrs={'class': 'form-control', 'accept': '.csv'})
    )
    
    delimiter_choices = [
        (',', 'Запятая (,)'),
        (';', 'Точка с запятой (;)'),
        ('\t', 'Табуляция (Tab)'),
        ('|', 'Вертикальная черта (|)'),
        (' ', 'Пробел'),
    ]
    
    encoding_choices = [
        ('utf-8', 'UTF-8'),
        ('cp1251', 'Windows-1251 (кириллица)'),
        ('latin1', 'Latin-1 (ISO-8859-1)'),
        ('ascii', 'ASCII'),
    ]
    
    delimiter = forms.ChoiceField(
        label='Разделитель полей',
        choices=delimiter_choices,
        initial=',',
        help_text='Выберите символ, которым разделены поля в CSV-файле',
        widget=forms.Select(attrs={'class': 'form-control'})
    )
    
    encoding = forms.ChoiceField(
        label='Кодировка файла',
        choices=encoding_choices,
        initial='utf-8',
        help_text='Выберите кодировку файла',
        widget=forms.Select(attrs={'class': 'form-control'})
    )
    
    has_header = forms.BooleanField(
        label='Первая строка содержит заголовки',
        initial=True,
        required=False,
        help_text='Отметьте, если первая строка файла содержит названия колонок',
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'})
    ) 