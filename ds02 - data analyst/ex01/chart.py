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
            SELECT
                DATE(event_time) AS date,
                COUNT(*) AS count_per_date
            FROM 
                customers
            WHERE
                event_type = 'purchase'
            GROUP BY 
                DATE(event_time)
            ORDER BY 
                DATE(event_time);
        """)

        dataset = dict(self.cursor.fetchall())

        dates = list(dataset.keys())
        values = list(dataset.values())

        plt.plot(dates, values, linestyle='-')
        plt.xlabel("Years")
        plt.ylabel("Number of customers")
        tick_positions = [0, len(dates) // 4, 2 * len(dates) // 4, 3 * len(dates) // 4]
        tick_labels = ["Oct", "Nov", "Dec", "Jan"]
        plt.xticks(tick_positions, tick_labels)
        plt.xlim(dates[0], dates[-1])
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
