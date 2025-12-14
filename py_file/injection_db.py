import pandas as pd
import os
from sqlalchemy import create_engine
import logging 
import time

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine = create_engine('mysql+pymysql://root:root@localhost:3306/inventory')

# This function will ingest the dataframe into database table
def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con = engine, if_exists = 'replace', index = False)


# load data in db
# This function will load the CSVs as dataframe and ingest into db
def load_raw_data():
    start = time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df = pd.read_csv('data/'+file)
            logging.info(f'Ingesting {file} in db')
            print(df.shape)
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time = (end - start)/60
    logging.info('-----------------Ingestion Complete!!-------------------')
    logging.info(f'\Total time taken: {total_time} minutes')


if __name__ == '__main__':
    load_raw_data()

