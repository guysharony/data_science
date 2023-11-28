import psycopg2
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


class Customers(Connection):
    def __init__(self, username: str, password: str, database: str) -> None:
        super().__init__(username, password, database)

    def remove_duplicates(self, table: str):
        temporary_table = f"{table}_temporary"

        print(f"[REMOVING ALL DUPLICATES FROM '{table}'] => ", end="", flush=True)

        self.cursor.execute(f"""
            CREATE TEMPORARY TABLE {temporary_table} AS
            SELECT DISTINCT *
            FROM {table};
        """)

        self.cursor.execute(f"""
           TRUNCATE TABLE {table};
        """)

        self.cursor.execute(f"""
            INSERT INTO {table}
            SELECT *
            FROM {temporary_table};
        """)

        self.cursor.execute(f"""
            DROP TABLE {temporary_table};
        """)

        self.connect.commit()

        print("REMOVED")


def main():
    try:
        customer = Customers('piscineds', 'guysharony', 'mysecretpassword')
        customer.remove_duplicates('customers')
        customer.close()
    except Exception as err:
        print(f'Error: {err}')


if __name__ == '__main__':
    main()
