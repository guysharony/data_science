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

    def prices(self):
        self.cursor.execute(f"""
            SELECT
                price
            FROM
                customers
            WHERE
                event_type = 'purchase';
        """)

        prices = [price[0] for price in self.cursor.fetchall()]

        self.statistics(prices)
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        ax1.boxplot(
            prices,
            widths=0.8,
            vert=False,
            notch=True
        )
        ax1.set_yticks([])
        ax1.set_xlabel("price")

        boxprops = dict(
            facecolor='green',
            edgecolor='black'
        )
        medianprops = dict(
            linewidth=1,
            color='black'
        )
        ax2.boxplot(
            prices,
            vert=False,
            widths=0.8,
            notch=True,
            boxprops=boxprops,
            medianprops=medianprops,
            showfliers=False,
            patch_artist=True
        )
        ax2.set_yticks([])
        ax2.set_xlabel("price")
        plt.tight_layout()
        plt.show()

def main():
    try:
        customer = Customers('piscineds', 'guysharony', 'mysecretpassword')
        customer.prices()
        customer.close()
    except Exception as err:
        print(f'Error: {err}')


if __name__ == '__main__':
    main()
