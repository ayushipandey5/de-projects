package com.olist.pipelines.order_reviews

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ColumnConstants {
  val TargetSchema : StructType = new StructType()
    .add("review_id",StringType)
    .add("order_id",StringType)
    .add("score",IntegerType)
    .add("comment_message",StringType)
    .add("creation_date",TimestampType)
    .add("answer_timestamp",TimestampType)
    .add("event_d",DateType)

  val dedupPartitionCol = Seq(col("review_id"), col("order_id"))

  val dedupOrderCol = Seq(col("review_answer_timestamp").desc)
}
