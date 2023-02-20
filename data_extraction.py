import pandas as pd
import tabula

from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self):
        pass
    
    def read_rds_table(self, connector_instance, table_name):

        return pd.read_sql_table(table_name, connector_instance)
    
    def retrieve_pdf_data(self, pdf_link):
        df = tabula.read_pdf(pdf_link, pages="all", multiple_tables=False)
        return df[0]


