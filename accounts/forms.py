from django import forms
from django.contrib.auth.models import User
from django.utils.translation import gettext_lazy as _
from .models import UserProfile
from .utils import clean_password

class UserForm(forms.ModelForm):
    """Форма для редактирования пользователя"""
    password = forms.CharField(widget=forms.PasswordInput(attrs={'class': 'form-control'}), required=False, label=_("Пароль"))
    
    class Meta:
        model = User
        fields = ['username', 'password', 'first_name', 'last_name', 'email']
        labels = {
            'username': _('Имя пользователя'),
            'first_name': _('Имя'),
            'last_name': _('Фамилия'),
            'email': _('Email')
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
        
        # Делаем поля first_name и last_name необязательными
        self.fields['first_name'].required = False
        self.fields['last_name'].required = False
        
        # Если это создание нового пользователя, пароль обязателен
        if not self.instance or not self.instance.pk:
            self.fields['password'].required = True
            self.fields['password'].help_text = _("Пароль обязателен для нового пользователя")
        else:
            self.fields['password'].help_text = _("Оставьте пустым, чтобы не менять пароль")
    
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
            raise forms.ValidationError(_("Пароль обязателен для нового пользователя"))
        
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

        # Если это создание нового пользователя, пароль обязателен
        if not self.instance or not self.instance.pk:
            if not password:
                raise forms.ValidationError(_("Пароль обязателен для нового пользователя"))
            from django.contrib.auth.hashers import make_password
            user.password = make_password(password)
        else:
            # Если это редактирование существующего пользователя
            if password:
                # Если пароль не пустой, обновляем его
                from django.contrib.auth.hashers import make_password
                user.password = make_password(password)
            # Если пароль пустой, не трогаем его (оставляем старый)
        
        if commit:
            if not password and (self.instance and self.instance.pk):
                # При редактировании с пустым паролем не обновляем поле password
                user.save(update_fields=['username', 'first_name', 'last_name', 'email'])
            else:
                # При создании или изменении пароля сохраняем все поля
                user.save()
        
        return user

class UserProfileForm(forms.ModelForm):
    """Форма для редактирования профиля пользователя"""
    # Добавляем поля пользователя
    first_name = forms.CharField(
        max_length=30,
        required=False,
        label=_('Имя'),
        widget=forms.TextInput(attrs={'class': 'form-control'})
    )
    last_name = forms.CharField(
        max_length=30,
        required=False,
        label=_('Фамилия'),
        widget=forms.TextInput(attrs={'class': 'form-control'})
    )
    email = forms.EmailField(
        label=_('Email'),
        widget=forms.EmailInput(attrs={'class': 'form-control'})
    )
    
    class Meta:
        model = UserProfile
        fields = ['department', 'position', 'phone_number']
        labels = {
            'department': _('Отдел'),
            'position': _('Должность'),
            'phone_number': _('Телефон')
        }
        widgets = {
            'department': forms.TextInput(attrs={'class': 'form-control'}),
            'position': forms.TextInput(attrs={'class': 'form-control'}),
            'phone_number': forms.TextInput(attrs={'class': 'form-control'})
        }
    
    def __init__(self, *args, **kwargs):
        self.user = kwargs.pop('user', None)
        super().__init__(*args, **kwargs)
        
        # Инициализируем поля пользователя, если пользователь передан
        if self.user:
            self.fields['first_name'].initial = self.user.first_name
            self.fields['last_name'].initial = self.user.last_name
            self.fields['email'].initial = self.user.email
    
    def clean_email(self):
        """Валидация email"""
        email = self.cleaned_data.get('email')
        if email:
            # Проверяем, что email уникален среди всех пользователей, кроме текущего
            if self.user and self.user.email == email:
                return email
            if User.objects.filter(email=email).exclude(pk=self.user.pk if self.user else None).exists():
                raise forms.ValidationError(_("Пользователь с таким email уже существует"))
        return email
    
    def clean_first_name(self):
        """Валидация имени"""
        first_name = self.cleaned_data.get('first_name', '')
        return first_name.strip()
    
    def clean_last_name(self):
        """Валидация фамилии"""
        last_name = self.cleaned_data.get('last_name', '')
        return last_name.strip()
    
    def clean_phone_number(self):
        """Валидация номера телефона"""
        phone_number = self.cleaned_data.get('phone_number', '')
        if phone_number:
            # Убираем все нецифровые символы для проверки
            digits_only = ''.join(filter(str.isdigit, phone_number))
            if len(digits_only) < 7:
                raise forms.ValidationError(_("Номер телефона должен содержать минимум 7 цифр"))
        return phone_number
    
    def save(self, commit=True):
        """Сохраняет форму и обновляет поля пользователя"""
        profile = super().save(commit=False)
        
        # Обновляем поля пользователя
        if self.user:
            self.user.first_name = self.cleaned_data['first_name']
            self.user.last_name = self.cleaned_data['last_name']
            self.user.email = self.cleaned_data['email']
            # Сохраняем только поля, которые мы изменили, не трогая пароль
            self.user.save(update_fields=['first_name', 'last_name', 'email'])
        
        if commit:
            profile.save()
        return profile

class PasswordChangeForm(forms.Form):
    """Форма для смены пароля"""
    old_password = forms.CharField(
        widget=forms.PasswordInput(attrs={'class': 'form-control'}),
        label=_("Текущий пароль")
    )
    new_password1 = forms.CharField(
        widget=forms.PasswordInput(attrs={'class': 'form-control'}),
        label=_("Новый пароль")
    )
    new_password2 = forms.CharField(
        widget=forms.PasswordInput(attrs={'class': 'form-control'}),
        label=_("Подтверждение пароля")
    )
    
    def __init__(self, user, *args, **kwargs):
        self.user = user
        super().__init__(*args, **kwargs)
    
    def clean_old_password(self):
        old_password = self.cleaned_data.get('old_password')
        if not self.user.check_password(old_password):
            raise forms.ValidationError(_("Неверный текущий пароль"))
        return old_password
    
    def clean_new_password2(self):
        password1 = self.cleaned_data.get('new_password1')
        password2 = self.cleaned_data.get('new_password2')
        if password1 and password2 and password1 != password2:
            raise forms.ValidationError(_("Пароли не совпадают"))
        return password2
    
    def clean_new_password1(self):
        password = self.cleaned_data.get('new_password1')
        # Убрано ограничение на минимальную длину пароля
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
        label=_("Включить двухфакторную аутентификацию"),
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'})
    )
    reset_totp = forms.BooleanField(
        required=False, 
        label=_("Сбросить настройки 2FA"),
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'})
    )
    
    def __init__(self, *args, **kwargs):
        self.user = kwargs.pop('user', None)
        super().__init__(*args, **kwargs)
        if self.user and hasattr(self.user, 'profile'):
            self.fields['totp_enabled'].initial = self.user.profile.totp_enabled 