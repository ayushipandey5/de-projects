"""
Olist Bronze Ingestion Pipeline
================================
To simulate OLTP for data ingestion in raw
    - Downloaded entire dataset using kaggle API into local
    - Scripts runs daily to enact ingestion and writes transactional data into GCS date based partitions
""" 
#  To DOWNLOAD KAGGLE DATA SET - RUN ONCE ONLY
# import kagglehub
# kagglehub.login()
# kagglehub.dataset_download('olistbr/brazilian-ecommerce', output_dir='../kaggle_data_raw/')

import pandas as pd 
from google.cloud import storage


geolocationDF = pd.read_csv('..kaggle_data_raw/olist_geolocation_dataset.csv')
productCategoryNameTranslationDF = pd.read_csv('olist-lakehouse/kaggle_data_raw/product_category_name_translation.csv')

def upload_to_gcs(bucket_name,data_frame,destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_f(source_file_name,)








