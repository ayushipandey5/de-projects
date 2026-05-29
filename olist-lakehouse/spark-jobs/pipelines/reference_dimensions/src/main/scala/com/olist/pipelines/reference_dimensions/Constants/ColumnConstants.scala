package com.olist.pipelines.reference_dimensions.Constants

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ColumnConstants {
  val geolocationTargetSchema : StructType = new StructType()
    .add("zip_code_prefix", StringType)
    .add("latitude", DoubleType)
    .add("longitude", DoubleType)
    .add("city", StringType)
    .add("state", StringType)
    .add("process_d", DateType)

  val dedupGeolocationPartitionByCols = Seq(col("zip_code_prefix"))
  val dedupGeolocationOrderByCols = Seq(col("process_d").desc_nulls_last)

  val geolocationRenameMap = Map(
    "geolocation_zip_code_prefix" -> "zip_code_prefix",
    "geolocation_lat" -> "latitude",
    "geolocation_lng" -> "longitude",
    "geolocation_city" -> "city",
    "geolocation_state" -> "state"
  )
  val geolocationTableProperties = Map(
    "format-version" -> "2",
    "write.delete.mode" -> "merge-on-read"
  )

  val productCatTransTargetSchema : StructType = new StructType()
    .add("category_name", StringType)
    .add("category_english_translation", StringType)
    .add("process_d", DateType)


  val dedupProductCatTransPartitionByCols = Seq(col("category_name"))
  val dedupProductCatTransOrderByCols = Seq(col("process_d").desc_nulls_last)

  val productCatTransRenameMap = Map(
    "product_category_name" -> "category_name",
    "product_category_name_english" -> "category_english_translation"
  )
}
