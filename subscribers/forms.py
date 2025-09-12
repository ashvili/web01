from django import forms

class CSVImportForm(forms.Form):
    """Объединенная форма для импорта данных из CSV-файла"""
    csv_file = forms.FileField(
        label='CSV-файл',
        help_text='Выберите CSV-файл с данными абонентов',
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
    

class SearchForm(forms.Form):
    """Форма для поиска абонентов"""
    phone_number = forms.CharField(
        label='Номер телефона',
        required=False,
        widget=forms.TextInput(
            attrs={
                'class': 'form-control',
                'placeholder': 'Введите номер телефона',
                'pattern': '[0-9]*',
                'title': 'Можно вводить только цифры',
                'oninput': 'this.value = this.value.replace(/[^0-9]/g, "")'
            }
        )
    )
    
    full_name = forms.CharField(
        label='ФИО',
        required=False,
        widget=forms.TextInput(
            attrs={
                'class': 'form-control',
                'placeholder': 'Введите имя, фамилию или отчество',
            }
        )
    )
    
    passport = forms.CharField(
        label='Номер паспорта',
        required=False,
        widget=forms.TextInput(
            attrs={
                'class': 'form-control',
                'placeholder': 'Введите номер паспорта',
            }
        ),
        help_text='Номер паспорта хранится в поле Memo1'
    )
    
    address = forms.CharField(
        label='Адрес',
        required=False,
        widget=forms.TextInput(
            attrs={
                'class': 'form-control',
                'placeholder': 'Введите адрес',
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