import time
import copy
import sys
from config import dbname, user, password, host, request_frequency
from services.crb_currencies import get_currency
from services.db import DataBaseManager
from services.google_sheets_api import get_values_from_sheet
from prettytable import PrettyTable

SERVICE_ACCOUNT_FILE = 'services/credentials.json'
SAMPLE_SPREADSHEET_ID = '1fnRF8pPsQkJ8JLYrDJXtoOrV0NNub82yygvABIJEk68'
SAMPLE_RANGE_NAME = 'Лист1!A1:D'


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
    dict_insets = {}
    for value in values:
        if value[0] in numbers_from_values - numbers_from_old_values:
            dict_insets[value[0]] = value

    differences['INSERT'] = list(dict_insets.values())
    differences['DELETE'] = numbers_from_old_values - numbers_from_values

    return differences


def add_col_rub(values: list, usd_rate: float):
    """Добавление 3-ей колонкой суммы в рублях"""
    values = values.copy()
    for i in range(len(values)):
        try:
            price_in_rub = round(float(values[i][2]) * usd_rate, 2)
            values[i].insert(3, price_in_rub)
        except (IndexError, TypeError, ValueError):
            values[i].insert(3, None)


def main(mode=None):
    print("Привет!"
          "\nДанный скрипт в режиме реального времени считывает информацию из Google Sheets и переносит в базу данных")

    if not all((dbname, user, password, host, request_frequency)):
        print('[ERROR] Проверьте правильность ввода информации в файл config.py.')
        exit(1)

    # Получаем котировку доллара
    usd_rate = get_currency('USD')
    if not usd_rate:
        print('[INFO] Котировка валюты не получена, будет использовано значение 1')
        usd_rate = 1.0

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
                # Режим сверки и обновления данных по строчно
                dict_differences = deep_compare(values, values_old)
                values_old = copy.deepcopy(values)
                # Добавление 3-ей колонкой суммы в рублях
                add_col_rub(dict_differences['UPDATE'], usd_rate)
                add_col_rub(dict_differences['INSERT'], usd_rate)
                # Операции с БД
                database.update_values(dict_differences['UPDATE'])
                database.insert_values(dict_differences['INSERT'])
                database.delete_values(dict_differences['DELETE'])

            else:
                # Режим полной замены таблицы при любом изменения документа Google Sheets
                values_old = copy.deepcopy(values)
                add_col_rub(values, usd_rate)
                database.clean_table()
                database.insert_values(values)

            print(f"[INFO] {time.ctime()} Данные были обновлены")

        time.sleep(request_frequency)


def show_table():
    """Выводит красиво таблицу в консоль"""
    if not all((dbname, user, password, host)):
        print('[ERROR] Проверьте правильность ввода информации в файл config.py.')
        exit(1)
    database = DataBaseManager(dbname, user, password, host)
    field_names = get_values_from_sheet(SERVICE_ACCOUNT_FILE, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)[0]
    x = PrettyTable()
    field_names.insert(3, 'cтоимость, руб.')
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
