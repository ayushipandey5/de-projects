package com.olist.pipelines.type1_scd.Constants
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ColumnConstants {

  val CustomersColMap : Map[String,String] = Map(
    "customer_zip_code_prefix"     -> "zip_code_prefix",
    "customer_city"                -> "city",
    "customer_state"               -> "state",
    "_created_dttm"                -> "created_date_time"
  )

  val dedupCustomersPartitionByCols = Seq(col("customer_id"))
  val dedupCustomersOrderByCols = Seq(col("created_date_time").desc , col("priority"))

  val CustomersTargetSchema : StructType = new StructType()
    .add("customer_id" , StringType)
    .add("customer_unique_id", StringType)
    .add("zip_code_prefix" , StringType)
    .add("city", StringType)
    .add("state", StringType)
    .add("created_date_time" , TimestampType)
    .add("event_d" , DateType)

  val customersIcebergJoinCols = Seq("customer_id")
  val customersIcebergUpdateCols = Seq("customer_unique_id","zip_code_prefix","city","state","created_date_time","event_d")


  val SellersColMap : Map[String,String] = Map(
    "seller_zip_code_prefix"     -> "zip_code_prefix",
    "seller_city"                -> "city",
    "seller_state"               -> "state",
    "_created_dttm"                -> "created_date_time"
  )

  val dedupSellersPartitionByCols = Seq(col("seller_id"))
  val dedupSellersOrderByCols = Seq(col("created_date_time").desc , col("priority"))


  val SellersTargetSchema : StructType = new StructType()
    .add("seller_id" , StringType)
    .add("zip_code_prefix" , StringType)
    .add("city", StringType)
    .add("state", StringType)
    .add("created_date_time" , TimestampType)
    .add("event_d" , DateType)

  val ProductsColMap : Map[String,String] = Map(
    "product_category_name" -> "category",
    "product_name_lenght" -> "name_length",
    "product_description_lenght" -> "description_length",
    "product_photos_qty" -> "number_of_photos",
    "product_weight_g" -> "weight",
    "product_length_cm" -> "length",
    "product_height_cm" -> "height",
    "product_width_cm" -> "width",
    "_created_dttm"   -> "created_date_time"
  )

  val dedupProductsPartitionByCols = Seq(col("product_id"))
  val dedupProductsOrderByCols = Seq(col("created_date_time").desc , col("priority"))

  val ProductsTargetSchema : StructType = new StructType()
    .add("product_id" , StringType)
    .add("category" , StringType)
    .add("name_length" , IntegerType)
    .add("description_length" , IntegerType)
    .add("number_of_photos" , IntegerType)
    .add("weight", DoubleType)
    .add("length", DoubleType)
    .add("height", DoubleType)
    .add("width", DoubleType)
    .add("created_date_time" , TimestampType)
    .add("event_d" , DateType)

}
