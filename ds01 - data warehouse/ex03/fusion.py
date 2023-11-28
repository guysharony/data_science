import csv
import psycopg2
from os import path
from sys import argv
from abc import ABC
from abc import abstractmethod


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
    
    @property
    def tables(self):
        self.cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public';
        """)
        return [table[0] for table in (self.cursor.fetchall() or [])]

    def add_columns_to_table(self, name: str, colomns: dict[str, str]):
        query = ",".join([f"ADD COLUMN {k} {v}" for k, v in colomns.items()])

        self.cursor.execute(f"""
            ALTER TABLE {name} ({query})
        """)

    def insert_to_table(self, name: str, row: list[str]):
        values = [None if len(v) == 0 else v for v in row]
        pre_values = ', '.join('%s' for _ in range(len(values)))
        query = f"INSERT INTO {name} VALUES ({pre_values})"
        self.cursor.execute(query, values)

class Customers(Connection):
    def __init__(self, username: str, password: str, database: str) -> None:
        super().__init__(username, password, database)

    def combine_tables(self, destination: str, source: str):
        print(f"[MERGING '{source}' into '{destination}'] => ", end="", flush=True)

        self.add_columns_to_table(destination, {
            "category_id": "BIGINT",
            "category_code": "VARCHAR(255)",
            "brand": "VARCHAR(255)"
        })

        self.cursor.execute(f"""
            UPDATE {destination} c
            SET category_id = i.category_id,
                category_code = i.category_code,
                brand = i.brand
            FROM (
                SELECT
                    product_id,
                    MAX(category_id) AS category_id,
                    MAX(category_code) AS category_code,
                    MAX(brand) AS brand
                FROM
                    {source}
                GROUP BY
                    product_id
            ) i
            WHERE c.product_id = i.product_id;
        """)

        self.connect.commit()

        print('CREATED')
        return True


def main():
    try:
        customer = Customers('piscineds', 'guysharony', 'mysecretpassword')
        customer.combine_tables("customers", "items")
        customer.close()
    except Exception as err:
        print(f'Error: {err}')

if __name__ == '__main__':
    main()