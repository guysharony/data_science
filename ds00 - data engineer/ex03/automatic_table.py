from os import path
from os import listdir
from sys import argv
import pandas as pd
from abc import ABC, abstractmethod
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import DateTime
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

    def create_table_from_cvs(self, file: str):
        filename = path.basename(file)
        name = path.splitext(filename)[0]

        print(f"[CREATING TABLE - '{name}'] => ", end="", flush=True)

        if self.is_table(name):
            print('ALREADY EXISTS')
            return False

        data = pd.read_csv(file)
        data_types = {
            "event_time": DateTime(),
            "event_type": types.String(length=255),
            "product_id": types.Integer(),
            "price": types.Float(),
            "user_id": types.BigInteger(),
            "user_session": types.UUID(as_uuid=True)
        }
        data.to_sql(name, self.engine, index=False, dtype=data_types)

        print('CREATED')
        return True

    def create_tables_from_cvs_directory(self, directory: str):
        files = listdir(directory)

        for file in files:
            self.create_table_from_cvs(f'{directory}{file}')

def main():
    try:
        assert len(argv) == 2, "Path for customer directory missing."

        customer = Customer('guysharony', 'mysecretpassword', 'piscineds')
        customer.create_tables_from_cvs_directory(argv[1])
        customer.dispose()
    except Exception as err:
        print(f'Error: {err}')

if __name__ == '__main__':
    main()