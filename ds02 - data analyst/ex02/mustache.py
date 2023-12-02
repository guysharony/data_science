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

        return prices

    def average_cart_per_user(self):
        self.cursor.execute(f"""
            SELECT
                user_id,
                AVG(total_price) AS average_basket_price_per_customer
            FROM (
                SELECT
                    user_id,
                    user_session,
                    SUM(price) AS total_price
                FROM
                    customers
                WHERE
                    event_type = 'cart'
                GROUP BY
                    user_id,
                    user_session
            ) AS basket_totals
            GROUP BY
                user_id
            ORDER BY
                user_id;
        """)

        return [cart[1] for cart in self.cursor.fetchall()]

def main():
    try:
        customer = Customers('piscineds', 'guysharony', 'mysecretpassword')
        prices = customer.prices()
        average_cart_per_user = customer.average_cart_per_user()
        customer.close()

        fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(14, 6))
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

        ax3.boxplot(
            average_cart_per_user,
            vert=False,
            widths=0.8,
            notch=True,
            boxprops=boxprops,
            medianprops=medianprops,
            showfliers=False,
            patch_artist=True
        )
        ax3.set_yticks([])
        ax3.set_xlabel("price")

        plt.tight_layout()
        plt.show()
    except Exception as err:
        print(f'Error: {err}')


if __name__ == '__main__':
    main()
