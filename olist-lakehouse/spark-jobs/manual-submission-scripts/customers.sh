JAR_GCS="gs://olist-confs/jars/type1-scd-dpp-2026-05-29.jar"

# Iceberg JARs — sourced directly from Maven Central via HTTPS.
MAVEN_BASE="https://storage-download.googleapis.com/maven-central/maven2/org/apache/iceberg"
ICEBERG_VERSION="1.9.1"
ICEBERG_RUNTIME_JAR="${MAVEN_BASE}/iceberg-spark-runtime-3.5_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.13-${ICEBERG_VERSION}.jar"
ICEBERG_GCP_BUNDLE_JAR="${MAVEN_BASE}/iceberg-gcp-bundle/${ICEBERG_VERSION}/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar"

gcloud dataproc batches submit spark \
--project=olist-lakehouse-497604 \
--service-account="71039564472-compute@developer.gserviceaccount.com" \
--batch=customers-iceberg-$(date +%Y%m%d-%H%M%S) \
--region=asia-south1 \
--version=2.2 \
--class=com.olist.pipelines.type1_scd.Main \
--jars="${JAR_GCS},${ICEBERG_RUNTIME_JAR},${ICEBERG_GCP_BUNDLE_JAR}" \
--files=gs://olist-confs/configs/customers_iceberg.config \
--properties="\
  spark.driver.extraJavaOptions=-DappConfig=./customers_iceberg.config -DrunType=iceberg,\
  spark.serializer=org.apache.spark.serializer.KryoSerializer,\
  spark.kryo.unsafe=false,\
  spark.sql.defaultCatalog=olist_lakehouse_catalog,\
  spark.sql.catalog.olist_lakehouse_catalog=org.apache.iceberg.spark.SparkCatalog,\
  spark.sql.catalog.olist_lakehouse_catalog.type=rest,\
  spark.sql.catalog.olist_lakehouse_catalog.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog,\
  spark.sql.catalog.olist_lakehouse_catalog.warehouse=gs://olist_lakehouse_catalog,\
  spark.sql.catalog.olist_lakehouse_catalog.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO,\
  spark.sql.catalog.olist_lakehouse_catalog.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager,\
  spark.sql.catalog.olist_lakehouse_catalog.header.x-goog-user-project=olist-lakehouse-497604,\
  spark.sql.catalog.olist_lakehouse_catalog.header.X-Iceberg-Access-Delegation=vended-credentials,\
  spark.sql.catalog.olist_lakehouse_catalog.gcs.oauth2.refresh-credentials-endpoint=https://oauth2.googleapis.com/token,\
  spark.sql.catalog.olist_lakehouse_catalog.rest-metrics-reporting-enabled=false,\
  spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

  # spark.sql.catalog.olist_lakehouse_catalog.gcs.oauth2.refresh-credentials-endpoint=https://oauth2.googleapis.com/token,\
