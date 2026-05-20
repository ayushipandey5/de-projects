package com.olist.pipelines.type1_scd.Constants
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object ColumnConstants {
  val customerColMap : Map[String,String] = Map(
    "customer_zip_code_prefix"     -> "zip_code_prefix",
    "customer_city"                -> "city",
    "customer_state"               -> "state",
    "_created_dttm"                -> "created_date_time"
  )

  val partitionByCols = Seq(col("state"),col("customer_unique_id"))
  val orderByCols = Seq(col("created_date_time").desc , col("priority"))

  val TargetSchema : StructType = new StructType()
    .add("customer_id" , StringType)
    .add("customer_unique_id", StringType)
    .add("zip_code_prefix" , StringType)
    .add("city", StringType)
    .add("state", StringType)
    .add("created_date_time" , TimestampType)
}
