from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account


def get_values_from_sheet(service_account_file: str, spreadsheet_id: str, range_name: str) -> list:
    """Получение строк таблицы в заданном диапазоне"""
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = service_account.Credentials.from_service_account_file(
        service_account_file, scopes=scopes)
    try:
        service = build('sheets', 'v4', credentials=creds)

        # Вызываем Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                    range=range_name).execute()
        # Получаем значения с листа
        values = result.get('values', [])

        if not values:
            print(f'[INFO] Возможно Google Sheets пустой. Ничего не найдено в заданном диапазоне {range_name}')

        return values

    except HttpError as err:
        print('[ERROR] Ошибка доступа к Google Sheets:', err)
        exit(1)

