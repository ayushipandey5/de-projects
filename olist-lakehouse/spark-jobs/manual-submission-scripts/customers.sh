# gcloud dataproc batches submit spark \
#   --service-account="dataprocsrvrls@olist-la.iam.gserviceaccount.com" \
#   --batch=olist-customers-silver-$(date +%Y%m%d-%H%M%S) \
#   --region=africa-south1 \
#   --class=com.olist.pipelines.type1_scd.Main \
#   --jars=gs://olist-configs/jars/type1-scd-dpp-2026-05-26.jar \
#   --files=gs://olist-configs/configs/customers.config \
#   --subnet=default \
#   --history-server-cluster=projects/olist-la/regions/africa-south1/clusters/olist-phs-server \
#   --ttl=3d \
#   --properties="spark.sql.sources.partitionOverwriteMode=dynamic,spark.driver.extraJavaOptions=-DappConfig=./customers.config"



gcloud dataproc batches submit spark \
--project=olist-lakehouse-497604 \
--service-account="71039564472-compute@developer.gserviceaccount.com" \
--batch=customers-iceberg-$(date +%Y%m%d-%H%M%S) \
--region=asia-south1 \
--version=2.2 \
--class=com.olist.pipelines.type1_scd.Main \
--jars=gs://olist-confs/jars/type1-scd-dpp-2026-05-29.jar \
--files=gs://olist-confs/configs/customers_iceberg.config \
--properties="\
  spark.driver.extraJavaOptions=-DappConfig=./customers_iceberg.config -DrunType=iceberg,\
  spark.sql.catalog.defaultCatalog=olist_lakehouse_catalog,\
  spark.sql.catalog.olist_lakehouse_catalog=org.apache.iceberg.spark.SparkCatalog,\
  spark.sql.catalog.olist_lakehouse_catalog.type=rest,\
  spark.sql.catalog.olist_lakehouse_catalog.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog,\
  spark.sql.catalog.olist_lakehouse_catalog.warehouse=gs://olist_lakehouse_catalog,\
  spark.sql.catalog.olist_lakehouse_catalog.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager,\
  spark.sql.catalog.olist_lakehouse_catalog.header.x-goog-user-project=olist-lakehouse-497604,\
  spark.sql.catalog.olist_lakehouse_catalog.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO,\
  spark.sql.catalog.olist_lakehouse_catalog.header.X-Iceberg-Access-Delegation=vended-credentials,\
  spark.sql.catalog.olist_lakehouse_catalog.gcs.oauth2.refresh-credentials-endpoint=https://oauth2.googleapis.com/token,\
  spark.sql.catalog.olist_lakehouse_catalog.rest-metrics-reporting-enabled=false,\
  spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

