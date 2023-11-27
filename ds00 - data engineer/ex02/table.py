#"INSERT INTO data_2022_dec (event_time, event_type, product_id, price, user_id, user_session) VALUES ('2022-12-30 18:52:27 UTC', 'view', 5759913, 0.79, 594994377, '02da5faa-c6d1-4c55-9f99-9d52c3963900'::UUID)"

#"INSERT INTO data_2022_dec (event_time, event_type, product_id, price, user_id, user_session) VALUES ('2022-12-30 18:52:27 UTC', 'view', 5759913, 0.79, 594994377, '02da5faa-c6d1-4c55-9f99-9d52c3963900'::UUID)"

import re
import psycopg2
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
        return self.cursor.fetchall()

    def tables_pattern(self, pattern):
        return [
            table[0] for table in self.tables if re.match(pattern, table[0])
        ]

    def union_all(self, tables: list[str]):
        return " UNION ALL ".join(
            f"SELECT * FROM {table}" for table in tables
        )

class Customers(Connection):
    def __init__(self, username: str, password: str, database: str) -> None:
        super().__init__(username, password, database)

    def create_table_from_cvs(self, path: str):
        tables = self.tables_pattern(pattern)

        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} AS (
                {self.union_all(tables)}
            );
        """)

        self.connect.commit()


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