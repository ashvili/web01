#!/usr/bin/env python3
"""
Тестовый файл для проверки логики обработки пароля
"""

def clean_password(password):
    """
    Проверяет и очищает пароль от крайних непечатных символов
    
    Args:
        password: Строка пароля
        
    Returns:
        tuple: (is_empty, cleaned_password)
            - is_empty: True если пароль пустой или содержит только непечатные символы
            - cleaned_password: Очищенный пароль без крайних непечатных символов
    """
    if not password:
        return True, ""
    
    # Убираем крайние пробелы и непечатные символы
    cleaned = password.strip()
    
    # Проверяем, содержит ли пароль только непечатные символы
    if not cleaned or cleaned.isspace():
        return True, ""
    
    return False, cleaned

def test_password_logic():
    """Тестирует логику обработки пароля"""
    
    test_cases = [
        # (input, expected_is_empty, expected_cleaned)
        ("", True, ""),
        (None, True, ""),
        ("   ", True, ""),
        ("\t\n\r", True, ""),
        ("  \t\n\r  ", True, ""),
        ("password", False, "password"),
        ("  password  ", False, "password"),
        ("\tpassword\n", False, "password"),
        ("  \tpassword\n\r  ", False, "password"),
        ("  \t  \n\r  ", True, ""),
        ("p a s s w o r d", False, "p a s s w o r d"),
        ("\tpassword with spaces\n", False, "password with spaces"),
    ]
    
    print("Тестирование логики обработки пароля:")
    print("=" * 50)
    
    for i, (input_pass, expected_empty, expected_cleaned) in enumerate(test_cases, 1):
        is_empty, cleaned = clean_password(input_pass)
        
        status = "✅" if (is_empty == expected_empty and cleaned == expected_cleaned) else "❌"
        
        print(f"{status} Тест {i}:")
        print(f"   Вход: {repr(input_pass)}")
        print(f"   Ожидалось: empty={expected_empty}, cleaned={repr(expected_cleaned)}")
        print(f"   Получено:  empty={is_empty}, cleaned={repr(cleaned)}")
        print()

if __name__ == "__main__":
    test_password_logic()
