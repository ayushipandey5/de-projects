package com.olist.pipelines.orders.Constants

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import scala.collection.immutable.Nil

object ColumnConstants {
  val TargetSceham : StructType = StructType(
    StructField("order_id",StringType,false)::
      StructField("customer_id",StringType,false)::
      StructField("status",StringType,false)::
      StructField("purchase_timestamp",TimestampType,false)::
      StructField("delivered_carrier_timestamp",TimestampType,true)::
      StructField("delivered_customer_timestamp",TimestampType,true)::
      StructField("estimated_delivery_timestamp",TimestampType,true)::
      Nil
  )
  val orderStatusRankMap = Map(
    "created" -> 6,
    "approved" -> 5,
    "invoiced" -> 4,
    "processing" -> 3,
    "shipped" -> 2,
    "delivered" -> 1,
    "canceled" -> 1,
    "unavailable" -> 1
  )
  val statusWeightedColumn = coalesce(
    typedLit(orderStatusRankMap).getItem(col("order_status")),
    lit(99)
  )

  val dedupePartitionCols = Seq(col("order_id"))
  val dedupeOrderByCols = Seq(statusWeightedColumn.asc, col("order_purchase_timestamp").desc)
}
