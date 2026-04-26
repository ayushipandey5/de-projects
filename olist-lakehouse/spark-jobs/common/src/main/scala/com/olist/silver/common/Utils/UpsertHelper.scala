package com.olist.silver.common.Utils

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object UpsertHelper {
  protected val logger : Logger = LogManager.getLogger(getClass.getName)

  def execute(inputDF: DataFrame, sourcePath: String, partitionCol: String, partitionByCols: Seq[Column], orderByCols: Seq[Column])
            (implicit sparkSession: SparkSession) : DataFrame = {
    val partitionsToUpdate = inputDF.select(partitionCol).distinct().collect().map(_.getString(0))
    val coreDF = getCoreDF(sourcePath,partitionCol,partitionsToUpdate)

    inputDF.withColumn("priority", lit(1))
    coreDF.withColumn("priority", lit(2))

    val unionDF = inputDF.unionByName(coreDF)
    val windowSpecs = Window.partitionBy(partitionByCols:_*).orderBy(orderByCols:_*)
    val dedupDF = unionDF
      .withColumn("rank", row_number().over(windowSpecs))
      .filter(col("rank") === 1)
      .drop("rank","priority")
    dedupDF

  }

  private def getCoreDF(sourcePath: String, partitionCol: String, partitionsToUpdate: Array[String])
                       (implicit sparkSession: SparkSession) : DataFrame = {
    val coreDF = sparkSession.read.parquet(sourcePath)
      .filter(col(partitionCol).isin(partitionsToUpdate:_*))
    coreDF
  }
}
