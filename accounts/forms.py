from django import forms
from .models import UserProfile

class UserProfileForm(forms.ModelForm):
    """Форма для редактирования профиля пользователя"""
    class Meta:
        model = UserProfile
        fields = ['department', 'position', 'phone_number', 'items_per_page', 'theme']
        labels = {
            'department': 'Отдел',
            'position': 'Должность',
            'phone_number': 'Телефон',
            'items_per_page': 'Записей на странице',
            'theme': 'Тема оформления'
        }
        widgets = {
            'department': forms.TextInput(attrs={'class': 'form-control'}),
            'position': forms.TextInput(attrs={'class': 'form-control'}),
            'phone_number': forms.TextInput(attrs={'class': 'form-control'}),
            'items_per_page': forms.NumberInput(attrs={'class': 'form-control'}),
            'theme': forms.Select(attrs={'class': 'form-control'})
        }
    
    def save(self, commit=True):
        """Сохраняет форму и обновляет права пользователя"""
        profile = super().save(commit=False)
        if commit:
            profile.save()
            profile.update_permissions()
        return profile

from django import forms
from .models import UserProfile

class UserProfileForm(forms.ModelForm):
    """Форма для редактирования профиля пользователя"""
    class Meta:
        model = UserProfile
        fields = ['department', 'position', 'phone_number', 'items_per_page', 'theme']
        labels = {
            'department': 'Отдел',
            'position': 'Должность',
            'phone_number': 'Телефон',
            'items_per_page': 'Записей на странице',
            'theme': 'Тема оформления'
        }
        widgets = {
            'department': forms.TextInput(attrs={'class': 'form-control'}),
            'position': forms.TextInput(attrs={'class': 'form-control'}),
            'phone_number': forms.TextInput(attrs={'class': 'form-control'}),
            'items_per_page': forms.NumberInput(attrs={'class': 'form-control'}),
            'theme': forms.Select(attrs={'class': 'form-control'})
        }
    
    def save(self, commit=True):
        """Сохраняет форму и обновляет права пользователя"""
        profile = super().save(commit=False)
        if commit:
            profile.save()
            profile.update_permissions()
        return profile 