package com.olist.pipelines.customers.Constants
import org.apache.spark.sql.functions._

object ColumnConstants {
  val customerColMap : Map[String,String] = Map(
    "customer_zip_code_prefix"     -> "zip_code_prefix",
    "customer_city"                -> "city",
    "customer_state"               -> "state",
    "_created_dttm"                -> "created_date_time"
  )

  val partitionByCols = Seq(col("state"),col("customer_unique_id"))
  val orderByCols = Seq(col("created_date_time").desc , col("priority"))

}
