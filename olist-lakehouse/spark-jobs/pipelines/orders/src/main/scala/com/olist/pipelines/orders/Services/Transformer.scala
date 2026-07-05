package com.olist.pipelines.orders.Services

import com.olist.pipelines.orders.Constants.ColumnConstants.statusWeightedColumn
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Transformer {
  protected lazy val logger : Logger = LogManager.getLogger(this.getClass.getName)

  def execute(inputDF : DataFrame) : DataFrame = {

    val windowSpec = Window.partitionBy("order_id").orderBy(statusWeightedColumn.asc, col("order_purchase_timestamp").desc)
    val dedupedDF = inputDF.withColumn("rn", row_number().over(windowSpec))
      .filter(col("rn") === 1)
      .drop(col("rn"))


    val transformedDF = dedupedDF
      .withColumn("status",initcap(trim(col("order_status"))))
      .withColumn("purchase_timestamp", to_timestamp(col("order_purchase_timestamp"),"yyyy-MM-dd hh:mm:ss"))
      .withColumn("approved_at_timestamp", to_timestamp(col("order_approved_at"),"yyyy-MM-dd hh:mm:ss"))
      .withColumn("delivered_carrier_timestamp", to_timestamp(col("order_delivered_carrier_date"),"yyyy-MM-dd hh:mm:ss"))
      .withColumn("delivered_customer_timestamp", to_timestamp(col("order_delivered_customer_date"),"yyyy-MM-dd hh:mm:ss"))
      .withColumn("estimated_delivery_timestamp", to_timestamp(col("order_estimated_delivery_date"),"yyyy-MM-dd hh:mm:ss"))

    transformedDF
  }
}
