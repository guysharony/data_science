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

    def pie(self, table: str):
        self.cursor.execute(f"""
            SELECT event_type, COUNT(*) AS count
            FROM {table}
            WHERE user_session IS NOT NULL
            GROUP BY event_type
            ORDER BY count DESC;
        """)

        dataset = dict(self.cursor.fetchall())

        keys = list(dataset.keys())
        values = list(dataset.values())

        event_colors = {'view': 'blue', 'purchase': 'red', 'remove_from_cart': 'green', 'cart': 'orange'}
        plt.pie(values, labels=keys, autopct='%1.1f%%', startangle=0, colors=[event_colors.get(event, 'gray') for event in keys])
        plt.axis('equal')
        plt.show()


def main():
    try:
        customer = Customers('piscineds', 'guysharony', 'mysecretpassword')
        customer.pie('customers')
        customer.close()
    except Exception as err:
        print(f'Error: {err}')


if __name__ == '__main__':
    main()
