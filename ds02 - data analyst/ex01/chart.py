import math
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

        plt.figure()

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
                TO_CHAR(event_time, 'YYYY-MM') AS month,
                CAST((SUM(price) / 1000000.0 * 0.8) AS DECIMAL(10, 2)) AS price
            FROM 
                customers
            WHERE
                event_type = 'purchase'
                AND event_time <= '2023-01-31'
            GROUP BY 
                TO_CHAR(event_time, 'YYYY-MM')
            ORDER BY 
                TO_CHAR(event_time, 'YYYY-MM');
        """)

        dataset = dict(self.cursor.fetchall())

        months = [datetime.strptime(str(date), '%Y-%m').strftime('%b') for date in dataset.keys()]
        counts = [sale for sale in dataset.values()]

        plt.figure()

        plt.bar(months, counts)
        plt.ylabel('total sales in million of ₳')
        plt.xlabel('month')
        plt.tick_params(axis='both', color='white')

        plt.show()

    def spend_by_customer(self):
        self.cursor.execute(f"""
            SELECT
                transaction_date,
                AVG(daily_total) AS average_spent_of_users
            FROM (
                SELECT
                    user_id,
                    DATE(event_time) AS transaction_date,
                    SUM(price) AS daily_total
                FROM
                    customers
                WHERE
                    event_type = 'purchase'
                    AND event_time <= '2023-01-31'
                GROUP BY
                    user_id,
                    DATE(event_time)
            ) AS user_daily_totals
            GROUP BY
                transaction_date
            ORDER BY
                transaction_date;
        """)

        dataset = dict(self.cursor.fetchall())

        dates = [datetime.strptime(str(date), '%Y-%m-%d') for date in dataset.keys()]
        values = list(dataset.values())

        plt.figure()

        plt.plot(dates, values, linewidth=1)
        plt.fill_between(dates, values, color='blue')
        plt.ylabel("average spend/customers in ₳")

        months_ticks = [date for date in dates if date.day == 1]
        months_labels = [date.strftime('%b') for date in months_ticks if date.day == 1]
        plt.xticks(months_ticks, months_labels)
        plt.yticks(range(0, math.ceil(max(values) / 5) * 5 + 1, 5))

        plt.xlim(dates[0], dates[-1])
        plt.ylim(bottom=0)
        plt.grid(which='both', linestyle='-', linewidth='0')

        plt.show()

def main():
    try:
        customer = Customers('piscineds', 'guysharony', 'mysecretpassword')
        customer.customers()
        customer.sales()
        customer.spend_by_customer()
        customer.close()
    except Exception as err:
        print(f'Error: {err}')


if __name__ == '__main__':
    main()
