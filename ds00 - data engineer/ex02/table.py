#"INSERT INTO data_2022_dec (event_time, event_type, product_id, price, user_id, user_session) VALUES ('2022-12-30 18:52:27 UTC', 'view', 5759913, 0.79, 594994377, '02da5faa-c6d1-4c55-9f99-9d52c3963900'::UUID)"

#"INSERT INTO data_2022_dec (event_time, event_type, product_id, price, user_id, user_session) VALUES ('2022-12-30 18:52:27 UTC', 'view', 5759913, 0.79, 594994377, '02da5faa-c6d1-4c55-9f99-9d52c3963900'::UUID)"

import psycopg2
import pandas as pd
import csv
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
    
    def create_table(self, name: str, colomns: dict[str, str]):
        query = ",".join([f"{k} {v}" for k, v in colomns.items()])

        self.cursor.execute(f"""
            CREATE TABLE {name} ({query})
        """)

    def insert_to_table(self, name: str, row: list[str]):
        values = [None if len(v) == 0 else v for v in row]
        pre_values = ', '.join('%s' for _ in range(len(values)))
        query = f"INSERT INTO {name} VALUES ({pre_values})"
        self.cursor.execute(query, values)

class Customers(Connection):
    def __init__(self, username: str, password: str, database: str) -> None:
        super().__init__(username, password, database)

    def create_table_from_cvs(self, file: str):
        name = path.splitext(path.basename(file))[0]

        print(f"[CREATING TABLE - '{name}'] => ", end="", flush=True)

        if name in self.tables:
            print('ALREADY EXISTS')
            return False

        self.create_table(name, {
            "event_time": "TIMESTAMP WITHOUT TIME ZONE",
            "event_type": "VARCHAR(255)",
            "product_id": "INTEGER",
            "price": "FLOAT",
            "user_id": "BIGINT",
            "user_session": "UUID"
        })

        with open(file, 'r') as csvfile:
            csv_reader = csv.reader(csvfile)
            next(csv_reader)
            for row in csv_reader:
                self.insert_to_table(name, row)

        self.connect.commit()

        print('CREATED')
        return True


def main():
    try:
        assert len(argv) == 2, "Path for CVS file is missing."

        customer = Customers('piscineds', 'guysharony', 'mysecretpassword')
        customer.create_table_from_cvs(argv[1])
        customer.close()
    except Exception as err:
        print(f'Error: {err}')

if __name__ == '__main__':
    main()