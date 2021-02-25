import pymysql
import pymysql.cursors

from config import MYSQL_DB, MYSQL_USER, MYSQL_HOST, MYSQL_PWD


class Db:
    def connect(self):
        """
        Подключение к базе данных
        """
        self.conn = pymysql.connect(host=MYSQL_HOST,
                                    user=MYSQL_USER,
                                    password=MYSQL_PWD,
                                    db=MYSQL_DB,
                                    charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)

    def query(self, query, params=()):
        """
        Запрос к базе данных
        """
        try:
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            self.conn.commit()
        except pymysql.OperationalError:
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            self.conn.commit()
        finally:
            self.conn.close()
        return cursor

    def count(self, table_name):
        """
        Количество строк в таблице с данным именем
        """
        query = "SELECT COUNT(*) FROM %s"
        self.request = self.query(query, table_name)
        self.result = self.request.fetchone()
        print("Db().count(): Counted")
        return self.result


def test():
    """
    Тестовая функция
    """
    table_name = "ebs_users"
    db = Db()
    print(db.count(table_name))
