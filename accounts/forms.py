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
    # Добавляем поля пользователя
    first_name = forms.CharField(
        max_length=30,
        label='Имя',
        widget=forms.TextInput(attrs={'class': 'form-control'})
    )
    last_name = forms.CharField(
        max_length=30,
        label='Фамилия',
        widget=forms.TextInput(attrs={'class': 'form-control'})
    )
    email = forms.EmailField(
        label='Email',
        widget=forms.EmailInput(attrs={'class': 'form-control'})
    )
    
    class Meta:
        model = UserProfile
        fields = ['department', 'position', 'phone_number']
        labels = {
            'department': 'Отдел',
            'position': 'Должность',
            'phone_number': 'Телефон'
        }
        widgets = {
            'department': forms.TextInput(attrs={'class': 'form-control'}),
            'position': forms.TextInput(attrs={'class': 'form-control'}),
            'phone_number': forms.TextInput(attrs={'class': 'form-control'})
        }
    
    def __init__(self, *args, **kwargs):
        self.user = kwargs.pop('user', None)
        super().__init__(*args, **kwargs)
    
    def save(self, commit=True):
        """Сохраняет форму и обновляет поля пользователя"""
        profile = super().save(commit=False)
        
        # Обновляем поля пользователя
        if self.user:
            self.user.first_name = self.cleaned_data['first_name']
            self.user.last_name = self.cleaned_data['last_name']
            self.user.email = self.cleaned_data['email']
            self.user.save()
        
        if commit:
            profile.save()
        return profile

class PasswordChangeForm(forms.Form):
    """Форма для смены пароля"""
    old_password = forms.CharField(
        widget=forms.PasswordInput(attrs={'class': 'form-control'}),
        label="Текущий пароль"
    )
    new_password1 = forms.CharField(
        widget=forms.PasswordInput(attrs={'class': 'form-control'}),
        label="Новый пароль",
        help_text="Пароль должен содержать минимум 8 символов"
    )
    new_password2 = forms.CharField(
        widget=forms.PasswordInput(attrs={'class': 'form-control'}),
        label="Подтверждение пароля"
    )
    
    def __init__(self, user, *args, **kwargs):
        self.user = user
        super().__init__(*args, **kwargs)
    
    def clean_old_password(self):
        old_password = self.cleaned_data.get('old_password')
        if not self.user.check_password(old_password):
            raise forms.ValidationError("Неверный текущий пароль")
        return old_password
    
    def clean_new_password2(self):
        password1 = self.cleaned_data.get('new_password1')
        password2 = self.cleaned_data.get('new_password2')
        if password1 and password2 and password1 != password2:
            raise forms.ValidationError("Пароли не совпадают")
        return password2
    
    def clean_new_password1(self):
        password = self.cleaned_data.get('new_password1')
        if password and len(password) < 8:
            raise forms.ValidationError("Пароль должен содержать минимум 8 символов")
        return password
    
    def save(self):
        password = self.cleaned_data['new_password1']
        self.user.set_password(password)
        self.user.save()
        return self.user

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