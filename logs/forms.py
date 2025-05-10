from django import forms
from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType
from .models import UserActionLog
import datetime

class LogFilterForm(forms.Form):
    """Форма для фильтрации логов действий пользователей"""
    
    user = forms.ModelChoiceField(
        label='Пользователь',
        queryset=User.objects.all().order_by('username'),
        required=False,
        widget=forms.Select(attrs={
            'class': 'form-control',
            'placeholder': 'Выберите пользователя'
        })
    )
    
    action_type = forms.ChoiceField(
        label='Тип действия',
        choices=[('', '---')] + UserActionLog.ACTION_TYPES,
        required=False,
        widget=forms.Select(attrs={
            'class': 'form-control',
            'placeholder': 'Выберите тип действия'
        })
    )
    
    date_from = forms.DateField(
        label='Дата от',
        required=False,
        widget=forms.DateInput(attrs={
            'class': 'form-control',
            'type': 'date'
        })
    )
    
    date_to = forms.DateField(
        label='Дата до',
        required=False,
        widget=forms.DateInput(attrs={
            'class': 'form-control',
            'type': 'date'
        })
    )
    
    content_type = forms.ModelChoiceField(
        label='Тип объекта',
        queryset=ContentType.objects.all(),
        required=False,
        widget=forms.Select(attrs={
            'class': 'form-control',
            'placeholder': 'Выберите тип объекта'
        })
    )
    
    object_id = forms.CharField(
        label='ID объекта',
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Введите ID объекта'
        })
    )
    
    search_term = forms.CharField(
        label='Поиск',
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Поиск по пути запроса'
        })
    )
    
    ip_address = forms.CharField(
        label='IP адрес',
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Введите IP адрес'
        })
    )
    
    def clean_date_from(self):
        date_from = self.cleaned_data.get('date_from')
        if date_from:
            # Устанавливаем время начала дня
            return datetime.datetime.combine(date_from, datetime.time.min)
        return date_from
    
    def clean_date_to(self):
        date_to = self.cleaned_data.get('date_to')
        if date_to:
            # Устанавливаем время конца дня
            return datetime.datetime.combine(date_to, datetime.time.max)
        return date_to
    
    def get_queryset(self):
        """Возвращает QuerySet логов с применением всех фильтров формы"""
        queryset = UserActionLog.objects.all().order_by('-action_time')
        
        user = self.cleaned_data.get('user')
        action_type = self.cleaned_data.get('action_type')
        date_from = self.cleaned_data.get('date_from')
        date_to = self.cleaned_data.get('date_to')
        content_type = self.cleaned_data.get('content_type')
        object_id = self.cleaned_data.get('object_id')
        search_term = self.cleaned_data.get('search_term')
        ip_address = self.cleaned_data.get('ip_address')
        
        if user:
            queryset = queryset.filter(user=user)
        
        if action_type:
            queryset = queryset.filter(action_type=action_type)
        
        if date_from:
            queryset = queryset.filter(action_time__gte=date_from)
        
        if date_to:
            queryset = queryset.filter(action_time__lte=date_to)
        
        if content_type:
            queryset = queryset.filter(content_type=content_type)
        
        if object_id:
            queryset = queryset.filter(object_id=object_id)
        
        if search_term:
            queryset = queryset.filter(path__icontains=search_term)
        
        if ip_address:
            queryset = queryset.filter(ip_address__icontains=ip_address)
        
        return queryset 