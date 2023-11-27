from os import path
from sys import argv
import pandas as pd
from abc import ABC, abstractmethod
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import types


class Connection(ABC):
    @abstractmethod
    def __init__(self, username: str, password: str, database: str) -> None:
        super().__init__()
        self.engine = create_engine(f"postgresql://{username}:{password}@localhost:5432/{database}")

    def is_table(self, name: str):
        metadata = MetaData()
        metadata.reflect(self.engine)

        return name in metadata.tables
    
    def dispose(self):
        self.engine.dispose()

class Customer(Connection):
    def __init__(self, username: str, password: str, database: str) -> None:
        super().__init__(username, password, database)

    def create_table_from_cvs(self, file: str, name: str = None):
        if name is None:
            filename = path.basename(file)
            name = path.splitext(filename)[0]

        print(f"[CREATING TABLE - '{name}'] => ", end="", flush=True)

        if self.is_table(name):
            print('ALREADY EXISTS')
            return False

        data = pd.read_csv(file)
        data_types = {
            "product_id": types.Integer(),
            "category_id": types.BigInteger(),
            "category_code": types.String(length=255),
            "brand": types.String(length=255),
        }
        data.to_sql(name, self.engine, index=False, dtype=data_types)

        print('CREATED')
        return True

def main():
    try:
        assert len(argv) == 2, "Path for customer directory missing."

        customer = Customer('guysharony', 'mysecretpassword', 'piscineds')
        customer.create_table_from_cvs(argv[1], 'items')
        customer.dispose()
    except Exception as err:
        print(f'Error: {err}')

if __name__ == '__main__':
    main()