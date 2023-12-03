import psycopg2
import numpy as np
import pandas as pd
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

    def average_cart_price(self):
        self.cursor.execute(f"""
            SELECT user_id, AVG(price) AS avg_cart_price
            FROM customers
            WHERE event_type = 'cart'
            GROUP BY user_id;
        """)

        return [price[0] for price in self.cursor.fetchall()]

    def statistics(self, dataset: list[str]):
        count = len(dataset)
        mean = np.mean(dataset)
        std = np.std(dataset)
        min = np.min(dataset)
        percentile = np.percentile(dataset, [25, 50, 75])
        max = np.max(dataset)

        df = pd.DataFrame({
            'count': ["{:0.6f}".format(count)],
            'mean': ["{:0.6f}".format(mean)],
            'std': ["{:0.6f}".format(std)],
            'min': ["{:0.6f}".format(min)],
            '25%': ["{:0.6f}".format(percentile[0])],
            '50%': ["{:0.6f}".format(percentile[1])],
            '75%': ["{:0.6f}".format(percentile[2])],
            'max': ["{:0.6f}".format(max)]
        })
        transposed_df = df.transpose()
        print('\n'.join(transposed_df.to_string().split('\n')[1:]))

    def frequency(self):
        self.cursor.execute(f"""
            SELECT
                user_id,
                COUNT(*) AS frequency
            FROM
                customers
            WHERE
                event_type = 'purchase'
            GROUP BY
                user_id
            HAVING COUNT(*) < 40;
        """)

        return [frequency[1] for frequency in self.cursor.fetchall()]

    def spend(self):
        self.cursor.execute(f"""
            SELECT
                user_id,
                SUM(price) AS spend
            FROM
                customers
            WHERE
                event_type = 'purchase'
            GROUP BY
                user_id
            HAVING SUM(price) < 225;
        """)

        return [spend[1] for spend in self.cursor.fetchall()]

def main():
    try:
        customer = Customers('piscineds', 'guysharony', 'mysecretpassword')
        frequency = customer.frequency()
        spend = customer.spend()
        customer.close()

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        ax1.hist(frequency, bins=5, edgecolor='k')
        ax1.set_xlabel('frequency')
        ax1.set_ylabel('customers')
        ax1.set_xticks(range(0, 39, 10))
        ax1.grid(color='gray', linestyle='-', linewidth=0.5, zorder=0)
        ax1.set_axisbelow(True)

        ax2.hist(spend, bins=5, edgecolor='k')
        ax2.set_xlabel('monetary value in ₳')
        ax2.set_ylabel('customers')
        ax2.set_xticks(range(0, 225, 50))
        ax2.grid(color='gray', linestyle='-', linewidth=0.5, zorder=0)
        ax2.set_axisbelow(True)

        plt.tight_layout()
        plt.show()
    except Exception as err:
        print(f'Error: {err}')


if __name__ == '__main__':
    main()
