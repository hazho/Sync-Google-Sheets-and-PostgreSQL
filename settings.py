# Данные о Google Sheet
# Расположение credentials
SERVICE_ACCOUNT_FILE = './credentials.json'
# ID Google Sheet документа
SAMPLE_SPREADSHEET_ID = '1fnRF8pPsQkJ8JLYrDJXtoOrV0NNub82yygvABIJEk68'
# Колонки для мониторинга
# При изменении кол-ва колонок требуется внести изменения в каждый запрос к базе данных в файле services/db.py
# и изменить число колонок NUMS_COLUMNS
SAMPLE_RANGE_NAME = 'Лист1!A:D'
# -----------------
# Данные для подключения PostgreSQL
host = '127.0.0.1'
user = ''
password = ''
dbname = ''
# -----------------
# Частота запросов к Google Sheets документу в секундах
request_frequency = 10

