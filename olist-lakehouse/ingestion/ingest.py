"""
Olist Bronze Ingestion Pipeline
================================
To simulate OLTP for data ingestion in raw
    - Downloaded entire dataset using kaggle API into local
    - Scripts runs daily to enact ingestion and writes transactional data into GCS date based partitions
""" 
# #  To DOWNLOAD KAGGLE DATA SET - RUN ONCE ONLY
# import kagglehub
# kagglehub.login()
# kagglehub.dataset_download('olistbr/brazilian-ecommerce', output_dir='../kaggle_data_raw/')

import pandas as pd 
import gcsfs
from google.cloud.storage import Client , transfer_manager

raw_data_dir = '../kaggle_data_raw'
bucket_name = "something"

referenceDimensionTables = ["product_category_name_translation","olist_geolocation_dataset"]
typeOneDimensionTables = ["olist_customer_dataset","olist_sellers_dataset","olist_products_dataset"]

def upload_to_gcs(dataFrame, fileName):
    gsPath = f"gs://{bucket_name}/{fileName}"
    dataFrame.to_parquet(gsPath, engine='pyarrow')


# def ingestReferenceDimensions(tableNamesMap):
#     for table, path in tableNamesMap:
#         df = pd.read_csv(path)
#         df['process_d'] = pd.Timestamp.now()
#         upload_to_gcs(df, table, "append")


# def ingestTypeOneDimensions(tableNamesMap):
#     for table, path in tableNamesMap:
#         df = pd.read_csv(path)
#         upload_to_gcs(df, table, "merge")


def ingestAccumulatingSnapshotFact(tableNamesMap):
    for table, tsCol in tableNamesMap.items():
        path = f'{raw_data_dir}/{table}.csv'
        df = pd.read_csv(path)
        df[tsCol] = pd.to_datetime(df[tsCol])

        start_date = df[tsCol].min().date()
        end_date = df[tsCol].max().date()
        print(start_date)
        print(end_date)
        for load_date in pd.date_range(start_date, end_date):
            load_date = load_date.date()
            filtered_df = df[df[tsCol].dt.date == load_date]

            if not filtered_df.empty:
                fileName = f"{table}/prcs_d={load_date}/{table}.parquet"
                print(fileName)
                upload_to_gcs(filtered_df, fileName)


# def ingestTransactionalFact(tableNames, source_directory, dest_dir):


# def ingestLineItemFacts(tableNames, source_directory, dest_dir):


ingestAccumulatingSnapshotFact({"olist_orders_dataset": "order_purchase_timestamp"})





