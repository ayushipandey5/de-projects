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
from datetime import datetime
import gcsfs
from google.cloud import storage

raw_data_dir = '../kaggle_data_raw'
bucket_name = "olist-lakehouse-bronze-layer"

referenceDimensionTables = ["product_category_name_translation","olist_geolocation_dataset"]
typeOneDimensionTables = ["olist_customer_dataset","olist_sellers_dataset","olist_products_dataset"]

def upload_to_gcs(dataFrame, fileName):
    gsPath = f"gs://{bucket_name}/{fileName}"
    dataFrame.to_parquet(gsPath, engine='pyarrow')


def ingestReferenceDimensions(tableNames):
    load_date = datetime.now().strftime("%Y-%m-%d")
    for table in tableNames:
        path = f"{raw_data_dir}/{table}.csv"
        df = pd.read_csv(path)
        df['_ingested_at'] = datetime.now()
        fileName = f"{table}/prcs_d={load_date}/{table}.parquet"
        print(f"Ingesting Reference Snapshot: {fileName}")
        upload_to_gcs(df, fileName)

    
#  manual changes done to imitate Type1 in Silver, changing random data in the table
# adding timestamp column to tables in the first load order to identify the latest. For later changed records, manual ts changes made
def ingestTypeOneDimensions(tableNames,load_ts):
    load_ts = pd.to_datetime(load_ts)
    load_date = load_ts.date()
    for table in tableNames:
        path = f"{raw_data_dir}/{table}.csv"
        df = pd.read_csv(path)
        df['_created_dttm'] = load_ts
        fileName = f"{table}/prcs_d={load_date}/{table}.parquet"
        print(f"Ingesting Dimensions: {fileName}")
        upload_to_gcs(df, fileName)
    
   


def ingestFactwithTS(tableNamesMap):
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



def ingestLineItemFacts(tableNamesMap, factTableName, factTableTsCol):
    factPath = f"{raw_data_dir}/{factTableName}.csv"
    
    required_join_keys = list(set(tableNamesMap.values()))
    load_cols = required_join_keys + [factTableTsCol]
    
    factDF = pd.read_csv(factPath, usecols=load_cols)
    factDF[factTableTsCol] = pd.to_datetime(factDF[factTableTsCol])

    factDF['prcs_d'] = factDF[factTableTsCol].dt.date

    for table, frKey in tableNamesMap.items():
        childPath = f"{raw_data_dir}/{table}.csv"
        litmDF = pd.read_csv(childPath)

        mergedDF = pd.merge(
            litmDF, 
            factDF[[frKey, 'prcs_d']], 
            how="inner", 
            on=frKey
        )

        for load_date, group_df in mergedDF.groupby('prcs_d'):
            final_df = group_df.drop(columns=['prcs_d'])
            
            fileName = f"{table}/prcs_d={load_date}/{table}.parquet"
            upload_to_gcs(final_df, fileName)


# ingestFactwithTS({"olist_orders_dataset": "order_purchase_timestamp", "olist_order_reviews_dataset": "review_creation_date"})
# ingestLineItemFacts({"olist_order_items_dataset":"order_id","olist_order_payments_dataset":"order_id"}, "olist_orders_dataset", "order_purchase_timestamp" )
# ingestReferenceDimensions(["olist_geolocation_dataset","product_category_name_translation"])
# ingestTypeOneDimensions(["olist_customers_dataset","olist_products_dataset","olist_sellers_dataset"],"2018-01-05 02:02:21")



