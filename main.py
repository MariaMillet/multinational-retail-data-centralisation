from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


import sqlalchemy as db
from sqlalchemy import create_engine, engine_from_config, inspect
from sqlalchemy.engine import URL

def extract_clean_user_data():
    dc = DataCleaning()
    db = DatabaseConnector()
    df = dc.clean_user_data()
    db.upload_to_db(df, 'dim_users')

def extract_clean_card_details():
    dc = DataCleaning()
    db = DatabaseConnector()
    df = dc.clean_card_data()
    db.upload_to_db(df, 'dim_card_details')


def main():
    extract_clean_user_data()
    extract_clean_card_details()


if __name__ == "__main__":
    main()
    db = DatabaseConnector()
    DATABASE_TYPE = 'postgresql'
    DBAPI = 'psycopg2'
    HOST = 'localhost'
    USER = 'postgres'
    PASSWORD = 'AiCore'
    DATABASE = 'Sales_Data'
    PORT = 5432
    engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
    inspector = inspect(engine)
    print(inspector.get_table_names())