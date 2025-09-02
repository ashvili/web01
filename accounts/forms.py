from django import forms
from django.contrib.auth.models import User
from .models import UserProfile
from .utils import clean_password

class UserForm(forms.ModelForm):
    """Форма для редактирования пользователя"""
    password = forms.CharField(widget=forms.PasswordInput(attrs={'class': 'form-control'}), required=False, label="Пароль")
    
    class Meta:
        model = User
        fields = ['username', 'password', 'first_name', 'last_name', 'email']
        labels = {
            'username': 'Имя пользователя',
            'first_name': 'Имя',
            'last_name': 'Фамилия',
            'email': 'Email'
        }
        widgets = {
            'username': forms.TextInput(attrs={'class': 'form-control'}),
            'first_name': forms.TextInput(attrs={'class': 'form-control'}),
            'last_name': forms.TextInput(attrs={'class': 'form-control'}),
            'email': forms.EmailInput(attrs={'class': 'form-control'})
        }
    
    def __init__(self, *args, **kwargs):
        self.instance = kwargs.get('instance')
        super().__init__(*args, **kwargs)
        
        # Если это создание нового пользователя, пароль обязателен
        if not self.instance or not self.instance.pk:
            self.fields['password'].required = True
            self.fields['password'].help_text = "Пароль обязателен для нового пользователя"
        else:
            self.fields['password'].help_text = "Оставьте пустым, чтобы не менять пароль"
    
    def clean_password(self):
        """
        Валидация пароля:
        - Если поле пустое или содержит только непечатные символы, считаем пустым
        - Очищаем от крайних непечатных символов
        - При создании пользователя пароль обязателен
        """
        password = self.cleaned_data.get('password')
        is_empty, cleaned_password = clean_password(password)
        
        # Если это создание нового пользователя и пароль пустой
        if (not self.instance or not self.instance.pk) and is_empty:
            raise forms.ValidationError("Пароль обязателен для нового пользователя")
        
        if is_empty:
            # Возвращаем пустую строку для пустого пароля
            return ""
        
        # Возвращаем очищенный пароль
        return cleaned_password
    
    def save(self, commit=True):
        """
        Переопределяем сохранение для правильной обработки пароля
        """
        user = super().save(commit=False)
        
        # Получаем очищенный пароль
        password = self.cleaned_data.get('password')

        # Если пароль не пустой, обновляем его
        if password:
            from django.contrib.auth.hashers import make_password
            user.password = make_password(password)

        # Если это создание нового пользователя, пароль обязателен
        if not self.instance or not self.instance.pk:
            if not password:
                raise forms.ValidationError("Пароль обязателен для нового пользователя")
            user.password = make_password(password)
        
        if commit:
            if not password:
                user.save(update_fields=['username', 'first_name', 'last_name', 'email'])
            else:
                user.save()
        
        return user

class UserProfileForm(forms.ModelForm):
    """Форма для редактирования профиля пользователя"""
    class Meta:
        model = UserProfile
        fields = ['user_type', 'department', 'position', 'phone_number', 'can_import_data', 'can_export_data', 'can_view_logs']
        labels = {
            'user_type': 'Тип пользователя',
            'department': 'Отдел',
            'position': 'Должность',
            'phone_number': 'Телефон',
            'can_import_data': 'Может импортировать данные',
            'can_export_data': 'Может экспортировать данные',
            'can_view_logs': 'Может просматривать логи'
        }
    
    def save(self, commit=True):
        """Сохраняет форму и обновляет права пользователя"""
        profile = super().save(commit=False)
        if commit:
            profile.save()
            profile.update_permissions()
        return profile

class TOTPForm(forms.Form):
    """Форма для управления 2FA"""
    totp_enabled = forms.BooleanField(
        required=False, 
        label="Включить двухфакторную аутентификацию",
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'})
    )
    reset_totp = forms.BooleanField(
        required=False, 
        label="Сбросить настройки 2FA",
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'})
    )
    
    def __init__(self, *args, **kwargs):
        self.user = kwargs.pop('user', None)
        super().__init__(*args, **kwargs)
        if self.user and hasattr(self.user, 'profile'):
            self.fields['totp_enabled'].initial = self.user.profile.totp_enabled 