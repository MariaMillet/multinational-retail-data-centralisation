#%%
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd
import datetime

class DataCleaning:
    def __init__(self):
        self.db_connector = DatabaseConnector()
        self.data_extractor = DataExtractor()

    def standardise_phone_number(self, phone_number, country):
        if country == "US":
            if phone_number.startswith("+1"):
                phone_number = phone_number[2:]
        elif country == "GB" or country == "DE":
            if phone_number.startswith("+44") or phone_number.startswith("+49"):
                phone_number = phone_number[3:]
                
        phone_number = ''.join(phone_number.split())
        phone_number = ''.join(phone_number.split('.'))
        phone_number = ''.join(phone_number.split('('))
        phone_number = ''.join(phone_number.split(')'))
        phone_number = ''.join(phone_number.split('-'))
        phone_number = phone_number.split('x')[0]
        if phone_number.startswith("0"):
                phone_number = phone_number[1:]

        if country == "US":
            phone_number = f"001{phone_number}"
        elif country == "DE":
            phone_number = f"0049{phone_number}"
        else:
            phone_number = f"0044{phone_number}"

        if len(phone_number) >14 or len(phone_number) < 13:
            phone_number = float('nan')

        return phone_number

    def clean_user_data(self):
        tables = self.db_connector.list_db_tables()
        user_data = tables[1]
        engine = self.db_connector.init_db_engine()
        df = self.data_extractor.read_rds_table(engine,user_data)
        df.set_index('index', inplace=True)
        
        # remove NULL rows
        null_rows = df.eq(df.iloc[:,0], axis=0).all(axis=1)
        # print(f'removed {null_rows.sum()} of rows with all "NULL" entries')
        df = df[null_rows == False]

        # transform date_of_birth to a date format
        rows_wrong_dates = pd.to_datetime(df['date_of_birth'],dayfirst=True, errors='coerce').isna()
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'],dayfirst=True, errors='coerce')
        # remove rows filled with the wrong info
        # print(f'removing {df[rows_wrong_dates]}')
        df = df[~rows_wrong_dates]

        # transform join_date in a date format
        rows_wrong_dates = pd.to_datetime(df['join_date'],dayfirst=True, errors='coerce').isna()
        df['join_date'] = pd.to_datetime(df['join_date'],dayfirst=True, errors='coerce')
        # remove rows filled with the wrong info
        # print(f'removing {df[rows_wrong_dates]}')
        df = df[~rows_wrong_dates]

        # correcting GGB typo in the country code
        ggb_loc = df['country_code'] == "GGB"
        df.loc[df[ggb_loc].index, 'country_code'] = "GB"
        df['country_code'] = df['country_code'].astype('category')
        df['country'] = df['country'].astype('category')
        
        for country_code in df['country_code'].unique():
            ud = df['phone_number'][df['country_code'] == country_code].apply(lambda x: self.standardise_phone_number(x, country_code))
            df.update(ud)
        
        df = df.dropna(subset=["phone_number"])

        return df
    
    def clean_card_data(self):
        df = self.data_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

        # remove erroneous values
        erroneous = df.duplicated(keep=False)
        df.drop(df[erroneous].index, inplace=True)
        rows_wrong_dates = pd.to_datetime(df['date_payment_confirmed'],dayfirst=True, errors='coerce').isna()
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'],dayfirst=True, errors='coerce')
        df.drop(df[rows_wrong_dates].index, inplace=True)

        df['expiry'] = df['expiry_date'].apply(lambda x: f'01/{x[:2]}/20{x[-2:]}')
        df['expiry_date'] = pd.to_datetime(df['expiry'], dayfirst=True, errors='coerce')
        df.drop(columns='expiry', inplace=True)
        return df




# if __name__ == "__main__":
#     dc = DataExtractor()
#     df = dc.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

# cards = DataCleaning()
# # df = cards.clean_card_data()
# # %%
# df.head()
# # %%
# df.info()
# # %%
# df['card_provider'].unique()
# # %%
# rows_wrong_dates = pd.to_datetime(df['date_payment_confirmed'],dayfirst=True, errors='coerce').isna()
# # df['date_of_birth'] = pd.to_datetime(df['date_of_birth'],dayfirst=True, errors='coerce')
# # %%
# date_format = "%Y%m%d"
# pd.to_datetime(df['expiry'], dayfirst=True, errors='coerce', format=date_format)
# # %%
