from transliterate import translit


def convert_to_latin(input_string) -> str:
    """
    Проверяет входную строку на наличие кириллических символов и переводит их в латиницу.

    :param input_string: Входная строка для проверки и возможного преобразования.
    :return: Строка с кириллическими символами заменёнными на латинские или исходная строка, если она была полностью на латыни.
    """

    # Проверка на наличие кириллических символов
    if any('а' <= c <= 'я' or 'А' <= c <= 'Я' for c in input_string):
        # Преобразование в транслитерацию (латиницу)
        latin_string = translit(input_string, 'ru', reversed=True)
        return latin_string

    # Если нет кириллических символов — возвращаем исходную строку
    return input_string


def expand_range(text: str) -> list[str]:
    if '-' not in text:
        return [text]

    number_type = 'float' if '.' in text else 'int'  # text D00-D09, D80.0-D80.9

    letter = text[0]  # A, B, D etc
    start_code, end_code = text.split("-", 1)  # D00, D09

    if number_type == 'int':
        start = int(start_code[1:])  # 0
        end = int(end_code[1:])  # 9

        if end > 10 > start:
            raise ValueError(f'Промежуток из единиц и десятков {text}')

        if end < 10:
            return [f'{letter}0{x}' for x in range(start, end + 1)]
        else:
            return [f'{letter}{x}' for x in range(start, end + 1)]


    elif number_type == 'float':

        if start_code[0:3] != end_code[0:3]:
            raise ValueError(f'Префикс кода должен быть одинаковый {text}')

        start = int(start_code[-1])  # 0
        end = int(end_code[-1])  # 9

        prefix = text[0:3]
        return [f'{prefix}.{x}' for x in range(start, end + 1)]

    raise ValueError(f'Неизвестный тип числа {text}')


def code_type(code: str) -> tuple[str, str]:
    type_ = 'code' if '.' in code else 'group'
    return code, type_


def test_expand_range():
    print(expand_range('I10-I54'))
    print(expand_range('I00-I09'))
    print(expand_range('I09.0-I09.4'))
    try:
        print(expand_range('I09.0-I03.4'))
    except ValueError:
        pass


if __name__ == '__main__':
    test_expand_range()