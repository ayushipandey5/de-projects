package com.olist.pipelines.order_payments

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Constants {
  val TargetSchema : StructType = new StructType()
    .add("order_id", StringType)
    .add("payment_sequential", IntegerType)
    .add("payment_type",StringType)
    .add("payment_installments",IntegerType)
    .add("payment_value",DoubleType)
    .add("event_d",DateType)

  val dedupePartitionCols = Seq(col("order_id"),col("payment_sequential"))

  val dedupeOrderByCols = Seq(col("prcs_d").desc)
}
