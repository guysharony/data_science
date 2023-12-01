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

        dataset = [price[0] for price in self.cursor.fetchall()]
        self.statistics(dataset)

def main():
    try:
        customer = Customers('piscineds', 'guysharony', 'mysecretpassword')
        customer.prices()
        customer.close()
    except Exception as err:
        print(f'Error: {err}')


if __name__ == '__main__':
    main()
