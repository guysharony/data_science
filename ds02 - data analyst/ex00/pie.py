import re
import psycopg2
from abc import ABC
from abc import abstractmethod
import matplotlib.pyplot as plt


class Connection(ABC):
    @abstractmethod
    def __init__(self, dbname: str, user: str, password: str) -> None:
        super().__init__()
        self.connect = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password
        )
        self.cursor = self.connect.cursor()

    def close(self):
        self.cursor.close()
        self.connect.close()


class Customers(Connection):
    def __init__(self, username: str, password: str, database: str) -> None:
        super().__init__(username, password, database)

    def fetch(self, table: str):
        self.cursor.execute(f"""
            SELECT event_type, COUNT(*) AS count
            FROM {table}
            WHERE user_session IS NOT NULL
            GROUP BY event_type;
        """)

        return dict(self.cursor.fetchall())


def main():
    try:
        customer = Customers('piscineds', 'guysharony', 'mysecretpassword')
        dataset = customer.fetch('customers')
        customer.close()

        keys = list(dataset.keys())
        values = list(dataset.values())

        plt.pie(values, labels=keys, autopct='%1.1f%%', startangle=0)
        plt.axis('equal')
        plt.show()
    except Exception as err:
        print(f'Error: {err}')


if __name__ == '__main__':
    main()
