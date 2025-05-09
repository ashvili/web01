from django import forms
from django.contrib.auth.models import User
from .models import UserProfile

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