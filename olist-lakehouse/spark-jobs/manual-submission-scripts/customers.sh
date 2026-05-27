gcloud dataproc batches submit spark \
  --service-account="dataprocsrvrls@olist-la.iam.gserviceaccount.com" \
  --batch=olist-customers-silver-$(date +%Y%m%d-%H%M%S) \
  --region=africa-south1 \
  --class=com.olist.pipelines.type1_scd.Main \
  --jars=gs://olist-configs/jars/type1-scd-dpp-2026-05-26.jar \
  --files=gs://olist-configs/configs/customers.config \
  --subnet=default \
  --history-server-cluster=projects/olist-la/regions/africa-south1/clusters/olist-phs-server \
  --ttl=3d \
  --properties="spark.sql.sources.partitionOverwriteMode=dynamic,spark.driver.extraJavaOptions=-DappConfig=./customers.config"

