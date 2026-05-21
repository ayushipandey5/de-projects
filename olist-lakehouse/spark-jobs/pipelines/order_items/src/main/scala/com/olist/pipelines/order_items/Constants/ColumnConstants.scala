package com.olist.pipelines.order_items.Constants

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ColumnConstants {
  val TargetSchema : StructType = new StructType()
    .add("order_id", StringType)
    .add("order_item_id", StringType)
    .add("product_id", StringType)
    .add("seller_id", StringType)
    .add("shipping_limit_date", TimestampType)
    .add("price", DoubleType)
    .add("freight_value", DoubleType)
    .add("event_d", DateType)


  val dedupPartitionCol =  Seq(col("order_id"),col("order_item_id"))
  val dedupOrderCol = Seq(col("prcs_d").desc_nulls_last)
}
