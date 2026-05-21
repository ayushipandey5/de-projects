package com.olist.pipelines.type1_scd.Services

import com.olist.pipelines.type1_scd.Constants.ColumnConstants._
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object Transformer {
  protected lazy val logger : Logger = LogManager.getLogger(getClass.getName)

  def execute(inputDF : DataFrame, appName: String) : DataFrame = {
//    val renamedDF = inputDF.select(inputDF.columns.map{c =>
//      val newName = customerColMap.getOrElse(c,c)
//      col(c).as(newName)
//    }:_*)

    val transformedDF = appName match {
      case name if name.contains("customers") => inputDF
        .withColumnsRenamed(CustomersColMap)
        .withColumn("state", upper(trim(col("state"))))
        .withColumn("city",upper(trim(col("city"))))
        .withColumn("created_date_time", to_timestamp(col("created_date_time"),"yyyy-MM-ss hh:mm:ss"))
        .withColumn("event_d" , to_date(substring(col("created_date_time"),0,10),"yyyy-MM-dd"))

      case name if name.contains("sellers") => inputDF
        .withColumnsRenamed(SellersColMap)
        .withColumn("state", upper(trim(col("state"))))
        .withColumn("seller_id", trim(col("seller_id")))
        .withColumn("city",upper(trim(col("city"))))
        .withColumn("created_date_time", to_timestamp(col("created_date_time"),"yyyy-MM-ss hh:mm:ss"))
        .withColumn("event_d" , to_date(substring(col("created_date_time"),0,10),"yyyy-MM-dd"))

      case name if name.contains("products") => inputDF
        .withColumnsRenamed(ProductsColMap)
        .withColumn("category",trim(col("category")))
        .withColumn("created_date_time", to_timestamp(col("created_date_time"),"yyyy-MM-ss hh:mm:ss"))
        .withColumn("event_d" , to_date(substring(col("created_date_time"),0,10),"yyyy-MM-dd"))
    }

//    val transformedDF = renamedDF
//        .withColumn("state", upper(trim(col("state"))))
////      .withColumn("customer_id", substring(col("customer_id"),-4,4))
//        .withColumn("customer_id", expr("right(customer_id,4)"))
//        .withColumn("city",upper(trim(col("city"))))
//        .withColumn("created_date_time", col("created_date_time").cast(TimestampType))
//    transformedDF.show(5,false)
    transformedDF

  }
}
