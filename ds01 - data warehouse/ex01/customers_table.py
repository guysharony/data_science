import re
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

    @property
    def tables(self):
        self.cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public';
        """)
        return [table[0] for table in (self.cursor.fetchall() or [])]

    def tables_pattern(self, pattern):
        return [
            table for table in self.tables if re.match(pattern, table)
        ]

    def union_all(self, tables: list[str]):
        return " UNION ALL ".join(
            f"SELECT * FROM {table}" for table in tables
        )


class Customers(Connection):
    def __init__(self, username: str, password: str, database: str) -> None:
        super().__init__(username, password, database)

    def join_tables(self, table: str, pattern):
        tables = self.tables_pattern(pattern)

        print(f"[CREATING TABLE - '{table}'] => ", end="", flush=True)

        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} AS (
                {self.union_all(tables)}
            );
        """)

        self.connect.commit()

        print("CREATED")


def main():
    try:
        customer = Customers('piscineds', 'guysharony', 'mysecretpassword')
        customer.join_tables('customers', r'data_202\d_[a-zA-Z]{3}')
        customer.close()
    except Exception as err:
        print(f'Error: {err}')


if __name__ == '__main__':
    main()
