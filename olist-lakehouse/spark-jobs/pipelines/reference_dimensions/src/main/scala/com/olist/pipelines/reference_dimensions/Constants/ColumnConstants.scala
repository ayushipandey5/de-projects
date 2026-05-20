package com.olist.pipelines.reference_dimensions.Constants

import org.apache.spark.sql.types._

object ColumnConstants {
  val TargetSchema : StructType = StructType(
    StructField("zip_code_prefix", StringType, true)::
    StructField("latitude", DoubleType, false)::
    StructField("longitude", DoubleType, false)::
    StructField("city", StringType, false)::
    StructField("state", StringType, false)::
      Nil
  )
}
