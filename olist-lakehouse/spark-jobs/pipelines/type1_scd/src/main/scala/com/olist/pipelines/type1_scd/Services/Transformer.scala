package com.olist.pipelines.type1_scd.Services

import com.olist.pipelines.type1_scd.Constants.ColumnConstants.customerColMap
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object Transformer {
  protected lazy val logger : Logger = LogManager.getLogger(getClass.getName)

  def execute(inputDF : DataFrame) : DataFrame = {
    val renamedDF = inputDF.select(inputDF.columns.map{c =>
      val newName = customerColMap.getOrElse(c,c)
      col(c).as(newName)
    }:_*)

    val transformedDF = renamedDF
        .withColumn("state", upper(trim(col("state"))))
//      .withColumn("customer_id", substring(col("customer_id"),-4,4))
        .withColumn("customer_id", expr("right(customer_id,4)"))
        .withColumn("city",upper(trim(col("city"))))
        .withColumn("created_date_time", col("created_date_time").cast(TimestampType))
    transformedDF.show(5,false)
    transformedDF

  }
}
