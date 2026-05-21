package com.olist.pipelines.reference_dimensions.Services

import com.olist.pipelines.reference_dimensions.Constants.ColumnConstants._
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Transformer {
  protected lazy val logger : Logger = LogManager.getLogger(this.getClass.getName)

  def execute(inputDF : DataFrame, appName:String) : DataFrame = {

    val (transformedDF, dedupPartitionByCols, dedupOrderByCols, targetSchema) =
      appName match {
        case name if name.contains("geolocation") => ( inputDF
          .withColumnsRenamed(geolocationRenameMap)
          .withColumn("latitude", round(col("latitude"), 2))
          .withColumn("longitude", round(col("longitude"), 2))
          .withColumn("city", upper(trim(col("city"))))
          .withColumn("state", upper(trim(col("state"))))
          ,dedupGeolocationPartitionByCols, dedupGeolocationOrderByCols, geolocationTargetSchema)

        case name if name.contains("category") => (inputDF
        .withColumnsRenamed(productCatTransRenameMap)
        .withColumn("category_name", trim(lower(col("category_name"))))
        .withColumn("category_english_translation", trim(lower(col("category_english_translation"))))
        ,dedupProductCatTransPartitionByCols, dedupProductCatTransOrderByCols, productCatTransTargetSchema)
    }
    transformedDF
  }
}
