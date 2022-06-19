import requests
import xml.etree.ElementTree as ET


def get_currency(currency: str) -> [float, None]:
    """Получение курса валюты с сайта ЦРБ"""
    xml = requests.get('https://www.cbr.ru/scripts/XML_daily.asp').text
    valute = ET.fromstring(xml).find(f"./Valute[CharCode='{currency}']/Value")
    if valute is None:
        print(f'[WARNING] Валюта {currency} не найдена')
        return

    rate = valute.text.replace(',', '.')
    return float(rate)


if __name__ == '__main__':
    print(get_currency('USD'))
