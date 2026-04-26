package com.olist.pipelines.customers.Services

import com.olist.pipelines.customers.Constants.ColumnConstants.customerColMap
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Transformer {
  protected lazy val logger : Logger = LogManager.getLogger(getClass.getName)

  def execute(inputDF : DataFrame) : DataFrame = {
    val dedupedDF = inputDF.select()
    val renamedDF = inputDF.select(inputDF.columns.map{c =>
      val newName = customerColMap.getOrElse(c,c)
      col(c).as(newName)
    }:_*)

    val transformedDF = renamedDF
        .withColumn("state", upper(trim(col("state"))))
//      .withColumn("customer_id", substring(col("customer_id"),-4,4))
        .withColumn("customer_id", expr("right(customer_id,4)"))
        .withColumn("city",upper(trim(col("city"))))
    transformedDF.show(5,false)
    transformedDF

  }

  private def getCoreDF(sourceTableName: String, transformedDF: DataFrame, partitionCol: List[String], partitionColName: String)
                       (implicit sparkSession: SparkSession) : DataFrame = {
    val query = s"select * from $sourceTableName where $partitionColName in $partitionCol"
  }
}
