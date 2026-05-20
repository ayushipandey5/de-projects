package com.olist.pipelines.reference_dimensions.Services

import com.olist.pipelines.reference_dimensions.Constants.ColumnConstants._
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Transformer {
  protected lazy val logger : Logger = LogManager.getLogger(this.getClass.getName)

  def execute(inputDF : DataFrame) : DataFrame = {
    val transformedDF: DataFrame = inputDF.withColumn("latitude", round(col("geolocation_lat"), 2))
      .withColumn("longitude", round(col("geolocation_lng"), 2))
      .withColumn("city", upper(trim(col("geolocation_city"))))
      .withColumn("state", upper(trim(col("geolocation_state"))))
      .withColumnRenamed("geolocation_zip_code_prefix", "zip_code_prefix")
      .withColumnRenamed("__ingested_at","load_date_time")
    transformedDF
  }


}
