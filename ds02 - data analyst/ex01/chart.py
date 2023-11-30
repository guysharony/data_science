import psycopg2
from datetime import datetime
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

    def customers(self):
        self.cursor.execute(f"""
            SELECT
                DATE(event_time) AS date,
                COUNT(*) AS count_per_date
            FROM 
                customers
            WHERE
                event_type = 'purchase'
                AND event_time <= '2023-01-31'
            GROUP BY 
                DATE(event_time)
            ORDER BY 
                DATE(event_time);
        """)

        dataset = dict(self.cursor.fetchall())

        dates = [datetime.strptime(str(date), '%Y-%m-%d') for date in dataset.keys()]
        values = list(dataset.values())

        ax = plt.axes()
        ax.set_facecolor("#eaeaf3")
        ax.spines['top'].set_edgecolor('white')
        ax.spines['right'].set_edgecolor('white')
        ax.spines['bottom'].set_edgecolor('white')
        ax.spines['left'].set_edgecolor('white')
        ax.tick_params(axis='both', color='white')

        plt.plot(dates, values, linewidth=1, linestyle='-')
        plt.xlabel("Years")
        plt.ylabel("Number of customers")

        months_ticks = [date for date in dates if date.day == 1]
        months_labels = [date.strftime('%b') for date in months_ticks if date.day == 1]
        plt.xticks(months_ticks, months_labels)

        plt.xlim(dates[0], dates[-1])
        plt.grid(color='white')
        plt.show()

    def sales(self):
        self.cursor.execute(f"""
            SELECT
                DATE_TRUNC('month', event_time) AS month,
                COUNT(*) AS count_per_date
            FROM 
                customers
            WHERE
                event_type = 'purchase'
                AND event_time <= '2023-01-31'
            GROUP BY 
                DATE_TRUNC('month', event_time)
            ORDER BY 
                DATE_TRUNC('month', event_time);
        """)

        dataset = dict(self.cursor.fetchall())

        print(dataset)


def main():
    try:
        customer = Customers('piscineds', 'guysharony', 'mysecretpassword')
        customer.customers()
        customer.sales()
        customer.close()
    except Exception as err:
        print(f'Error: {err}')


if __name__ == '__main__':
    main()
