import yaml
import sqlalchemy as db
from sqlalchemy import create_engine, engine_from_config, inspect
from sqlalchemy.engine import URL


class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            config = yaml.safe_load(file)
        return config

    def init_db_engine(self):
        config = self.read_db_creds()
        url_object = URL.create(
            "postgresql+psycopg2",
            username=config['RDS_USER'],
            password=config['RDS_PASSWORD'],  # plain (unescaped) text
            host=config['RDS_HOST'],
            database=config['RDS_DATABASE'],
            port=config['RDS_PORT']
        )
        engine = create_engine(url_object )
        return engine

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df, table_name):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'AiCore'
        DATABASE = 'Sales_Data'
        PORT = 5432
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        df.to_sql(table_name, engine, if_exists='replace')
    



    

if __name__ == "__main__":
    db = DatabaseConnector()
    db_names = db.list_db_tables()
    print(db_names)


