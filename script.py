import time
import copy
import sys
from settings import *
from services.db import DataBaseManager
from services.google_sheets_api import get_values_from_sheet
from prettytable import PrettyTable


def deep_compare(values: list, values_old: list) -> dict:
    """Сравнение старых данных с новыми построчно"""
    differences = {'UPDATE': [],
                   'INSERT': [],
                   'DELETE': set()}

    for i in range(min(len(values), len(values_old))):
        if values[i] != values_old[i] and values[i][0] == values_old[i][0]:
            differences['UPDATE'].append(values[i])

    # Множество номеров строк в колонке "№"
    numbers_from_values = set([value[0] for value in values])
    numbers_from_old_values = set([value[0] for value in values_old])

    # Исключаем дубликаты номеров строк в колонке "№", добавляя последнюю из них
    dict_inserts = {}
    for value in values:
        if value[0] in numbers_from_values - numbers_from_old_values:
            dict_inserts[value[0]] = value

    differences['INSERT'] = list(dict_inserts.values())
    differences['DELETE'] = numbers_from_old_values - numbers_from_values

    return differences


def main(mode=None):
    print("Привет!"
          "\nДанный скрипт в режиме реального времени считывает информацию из Google Sheets и переносит в базу данных.")
    print('Режим работы:', end=' ')
    if mode:
        print('продуманный, сверка и обновление данных в таблице построчно')
    else:
        print('полная замены таблицы при любом изменении документа Google Sheets')
    if not all((dbname, user, password, host, request_frequency, SERVICE_ACCOUNT_FILE, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)):
        print('[ERROR] Проверьте правильность ввода информации в файл settings.py.')
        exit(1)

    # Подключаем базу данных
    database = DataBaseManager(dbname, user, password, host)
    database.create_table()

    # Анализируем данные из Google Sheets
    values_old = []
    while True:
        values = get_values_from_sheet(SERVICE_ACCOUNT_FILE, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)
        # Удаляем пустые строки и название колонок, проверяем колонку '№' на наличие числа
        values = [value for value in values if (value and value[0].isdecimal())]
        # Сравниваем с предыдущими данными
        if values != values_old:
            if mode == 'deep' and values_old:
                # Режим сверки и обновления данных построчно
                dict_differences = deep_compare(values, values_old)
                values_old = copy.deepcopy(values)

                # Операции с БД
                database.update_values(dict_differences['UPDATE'])
                database.insert_values(dict_differences['INSERT'])
                database.delete_values(dict_differences['DELETE'])

            else:
                # Режим полной замены таблицы при любом изменения документа Google Sheets
                values_old = copy.deepcopy(values)
                database.clean_table()
                database.insert_values(values)

            print(f"[INFO] {time.ctime()} Данные были обновлены")

        time.sleep(request_frequency)


def show_table():
    """Выводит красиво таблицу в консоль"""
    if not all((dbname, user, password, host, SERVICE_ACCOUNT_FILE, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)):
        print('[ERROR] Проверьте правильность ввода информации в файл settings.py.')
        exit(1)
    database = DataBaseManager(dbname, user, password, host)
    field_names = get_values_from_sheet(SERVICE_ACCOUNT_FILE, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)[0]
    x = PrettyTable()
    x.field_names = field_names
    x.add_rows(database.get_table_values())
    print(x)


if __name__ == '__main__':
    try:
        match sys.argv[1]:
            case 'show':
                show_table()
            case 'deep':
                main('deep')
            case _:
                print('Неизвестный аргумент. Используете: deep, show, без аргументов.')
    except IndexError:
        main()
