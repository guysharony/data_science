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

    def remove_customers_duplicates(self):
        print(f"[REMOVING DUPLICATES FROM TABLE - 'customers'] => ", end="", flush=True)

        self.cursor.execute(f"""
            CREATE TEMPORARY TABLE temporary_customers AS (
                SELECT event_time, event_type, product_id, price, user_id, user_session
                FROM (
                    SELECT *,
                        CASE
                            WHEN event_time - lag(event_time) OVER w <= INTERVAL '1 minute' THEN 0
                            ELSE 1
                        END AS is_first_occurrence
                    FROM customers
                    WINDOW w AS (PARTITION BY DATE_TRUNC('minute', event_time), event_type, product_id, price, user_id, user_session ORDER BY event_time)
                ) AS subquery
                WHERE is_first_occurrence = 1
            );
        """)

        self.cursor.execute(f"""
            TRUNCATE customers;
        """)

        self.cursor.execute(f"""
            INSERT INTO customers SELECT * FROM temporary_customers;
        """)

        self.cursor.execute(f"""
            DROP TABLE temporary_customers;
        """)

        self.connect.commit()

        print("REMOVED")


def main():
    try:
        customer = Customers('piscineds', 'guysharony', 'mysecretpassword')
        customer.remove_customers_duplicates()
        customer.close()
    except Exception as err:
        print(f'Error: {err}')


if __name__ == '__main__':
    main()
