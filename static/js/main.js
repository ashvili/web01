// Автоматическое закрытие сообщений об успехе через 3 секунды
document.addEventListener('DOMContentLoaded', function() {
    setTimeout(function() {
        const successMessages = document.querySelectorAll('.alert-success');
        successMessages.forEach(function(message) {
            const closeButton = message.querySelector('.btn-close');
            if (closeButton) {
                closeButton.click();
            }
        });
    }, 3000);

    // Обработка смены темы оформления
    const themeSelect = document.getElementById('theme-select');
    if (themeSelect) {
        themeSelect.addEventListener('change', function() {
            document.body.classList.remove('theme-light', 'theme-dark');
            document.body.classList.add('theme-' + this.value);
            
            // Сохранение выбора темы с помощью AJAX
            const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]').value;
            
            fetch('/accounts/set-theme/', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'X-CSRFToken': csrfToken
                },
                body: 'theme=' + this.value
            });
        });
    }
    
    // Инициализация всплывающих подсказок Bootstrap
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    const tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}); 