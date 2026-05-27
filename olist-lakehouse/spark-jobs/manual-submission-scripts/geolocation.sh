gcloud dataproc batches submit spark \
--service-account="dataprocsrvrls@olist-la.iam.gserviceaccount.com" \
--batch=olist-geolocation-silver-$(date +%Y%m%d-%H%M%S) \
--region=africa-south1 \
--subnet=default \
--class="com.olist.pipelines.reference_dimensions.Main" \
--jars=gs://olist-configs/jars/reference-dimensions-dpp-2026-05-26.jar \
--files="gs://olist-configs/configs/geolocation.config" \
--ttl=3d \
--properties="spark.sql.sources.partitionOverwriteMode=dynamic,spark.driver.extraJavaOptions=-DappConfig=./geolocation.config"