from django import forms
from django.contrib.auth.models import User
from django.contrib.auth.forms import UserCreationForm

from .models import UserProfile

class UserProfileForm(forms.ModelForm):
    """Форма для редактирования профиля пользователя"""
    first_name = forms.CharField(max_length=30, required=False, label='Имя')
    last_name = forms.CharField(max_length=30, required=False, label='Фамилия')
    email = forms.EmailField(max_length=254, required=False, label='Email')
    
    class Meta:
        model = UserProfile
        fields = ['department', 'position', 'phone_number', 'items_per_page', 'theme']
        labels = {
            'department': 'Отдел',
            'position': 'Должность',
            'phone_number': 'Телефон',
            'items_per_page': 'Записей на странице',
            'theme': 'Тема оформления',
        }
        widgets = {
            'theme': forms.Select(choices=[('light', 'Светлая'), ('dark', 'Темная')]),
            'items_per_page': forms.Select(choices=[(10, '10'), (25, '25'), (50, '50'), (100, '100')]),
        }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance and self.instance.user:
            self.fields['first_name'].initial = self.instance.user.first_name
            self.fields['last_name'].initial = self.instance.user.last_name
            self.fields['email'].initial = self.instance.user.email
    
    def save(self, commit=True):
        profile = super().save(commit=False)
        
        # Обновляем данные пользователя
        user = profile.user
        user.first_name = self.cleaned_data['first_name']
        user.last_name = self.cleaned_data['last_name']
        user.email = self.cleaned_data['email']
        
        if commit:
            user.save()
            profile.save()
        
        return profile 