import psycopg2
import psycopg2.extras


class DataBaseManager:
    """Взаимодействует с базой PosgreSQL"""
    TABLE_NAME = 'orders'

    def __init__(self, dbname, user, password, host, table_name=None):
        self.dbname = dbname
        self.user = user
        self._password = password
        self.host = host
        if table_name:
            self.TABLE_NAME = table_name
        self.connection = self._get_connection()

    def create_table(self):
        """Создание таблицы"""
        CREATE_TABLE = f"""CREATE TABLE {self.TABLE_NAME}(
                        id serial PRIMARY KEY,
                        number_row int,
                        order_number varchar(10),
                        price_usd varchar(10),
                        price_rub varchar(15),
                        delivery_date varchar(10));"""

        with self.connection.cursor() as cursor:
            if not self._is_table_exists(cursor):
                self._sql_execute(cursor, CREATE_TABLE)
                print(f'[INFO] Таблица {self.TABLE_NAME} создана успешно')
            else:
                ans = input(f'Таблица {self.TABLE_NAME} уже существует. Необходимо пересоздать. Продолжить?(Y/N):')
                if ans.strip().lower() == 'y':
                    self._sql_execute(cursor, f"""DROP TABLE {self.TABLE_NAME};""" + CREATE_TABLE)
                    print(f'[INFO] Таблица {self.TABLE_NAME} удалена и заново создана успешно')
                else:
                    print(f'[ERROR] Завершение, так как таблица {self.TABLE_NAME} уже существует.')
                    self._close_connection()
                    exit(1)

    def insert_values(self, values: list):
        """Вставка данных в таблицу"""
        if not values: return

        # Дополнение данных если не соответствует кол-ву столбцов в таблице
        for value in values:
            for _ in range(5 - len(value)):
                value.append(None)

        with self.connection.cursor() as cursor:
            query = f"INSERT INTO {self.TABLE_NAME}(number_row, order_number, price_usd, price_rub, delivery_date) VALUES %s"
            self._sql_multy_execute(cursor, query, values)

    def update_values(self, values: list):
        """Обновление значений по номеру"""
        if not values: return
        # Дополнение данных если не соответствует кол-ву столбцов в таблице
        for value in values:
            for _ in range(5 - len(value)):
                value.append(None)
            # Приведение колонки № к типу int
            value[0] = int(value[0])

        with self.connection.cursor() as cursor:
            query = f"""UPDATE {self.TABLE_NAME}
                        SET number_row = data.number_row,
                            order_number = data.order_number, 
                            price_usd = data.price_usd, 
                            price_rub = data.price_rub, 
                            delivery_date= data.delivery_date
                        FROM (VALUES %s) AS data (number_row, order_number, price_usd, price_rub, delivery_date)
                        WHERE {self.TABLE_NAME}.number_row = data.number_row"""
            self._sql_multy_execute(cursor, query, values)

    def clean_table(self):
        """Удалить все записи таблицы"""
        with self.connection.cursor() as cursor:
            self._sql_execute(cursor, f'DELETE FROM {self.TABLE_NAME};')

    def delete_values(self, rows: set):
        """Удаление строк по их номерам"""
        if not rows: return

        with self.connection.cursor() as cursor:
            self._sql_execute(cursor, f"DELETE FROM {self.TABLE_NAME} "
                                      f"WHERE number_row IN ({' ,'.join(rows)});")

    def get_table_values(self):
        """Возвращает содержимое таблицы TABLE_NAME"""
        with self.connection.cursor() as cursor:
            if self._is_table_exists(cursor):
                query = f"SELECT number_row,order_number, price_usd, price_rub,delivery_date FROM {self.TABLE_NAME} ORDER BY number_row"
                self._sql_execute(cursor, query)
                return cursor.fetchall()
            else:
                print(f'[ERROR] Таблицы {self.TABLE_NAME} нет существует.')
                exit(1)

    def _get_connection(self):
        """Подключение к базе данных"""
        try:
            connection = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self._password,
                database=self.dbname
            )
            connection.autocommit = True
            return connection
        except Exception as err:
            print("[ERROR] Не удалось подключиться к базе данных:", self.dbname)
            print(err)
            exit(1)

    def _close_connection(self):
        """Закрывает соединение с базой данных"""
        if getattr(self, 'connection'):
            self.connection.close()
            print("[INFO] Соединение с базой данных закрыто.")

    def _is_table_exists(self, cursor) -> bool:
        """Проверка наличия таблицы"""
        self._sql_execute(cursor, f"SELECT EXISTS(SELECT relname FROM pg_class WHERE relname = '{self.TABLE_NAME}');")
        return True if cursor.fetchone()[0] else False

    def _sql_execute(self, cursor, query: str):
        """Выполнение SQL запроса"""
        try:
            cursor.execute(query)
        except Exception as err:
            print(f"[ERROR] Неудачное выполнение SQL запроса в таблицу {self.TABLE_NAME}")
            print("[ERROR]", err)
            self._close_connection()
            exit(1)

    def _sql_multy_execute(self, cursor, query, values):
        """Выполнение SQL запроса к нескольким строкам сразу"""
        try:
            psycopg2.extras.execute_values(cursor, query, values)
        except Exception as err:
            print(f"[ERROR] Ошибка обновления/добавления данных в таблицу: {self.TABLE_NAME} !")
            print("[ERROR]", err)
            self._close_connection()
            exit(1)
