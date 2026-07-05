package com.olist.silver.common.Utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object UpsertHelper {
  protected val logger : Logger = LogManager.getLogger(getClass.getName)

  def execute(inputDF: DataFrame, sinkPath: String, partitionCol: String, partitionByCols: Seq[Column], orderByCols: Seq[Column], TargetSchema: StructType)
            (implicit sparkSession: SparkSession) : DataFrame = {

    val coreDF = if(partitionCol.isBlank){
      try {
        sparkSession.read.parquet(sinkPath)
      } catch {
        case e: org.apache.spark.sql.AnalysisException if e.getMessage().contains("UNABLE_TO_INFER_SCHEMA") =>
          logger.warn(s"Silver path $sinkPath is empty. Initializing empty DataFrame.")
          sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], TargetSchema)
        case e: Exception => throw e
      }
    }
    else {
      val partitionsToUpdate = inputDF.select(partitionCol).distinct().collect().map(_.getString(0))
      getCoreDF(sinkPath, partitionCol, partitionsToUpdate, TargetSchema)
    }
    val bronzeDF = inputDF.withColumn("priority", lit(1))
    val silverDF = coreDF.withColumn("priority", lit(2))

    val unionDF = bronzeDF.unionByName(silverDF)
    val windowSpecs = Window.partitionBy(partitionByCols:_*).orderBy(orderByCols:_*)
    val dedupDF = unionDF
      .withColumn("rank", row_number().over(windowSpecs))
      .filter(col("rank") === 1)
      .drop("rank","priority")
    dedupDF

  }

  def executeUsingIceberg(inputDF : DataFrame, coreTable : String ,joinCols : Seq[String], updateCols : Seq[String]) (implicit sparkSession: SparkSession) : Unit  = {
    inputDF.createOrReplaceTempView("incoming_records")
    val joinString = joinCols
      .map(c => s"target.$c = source.$c")
      .mkString(" AND ")

    val updateString = updateCols
      .map( c => s"target.$c = source.$c")
      .mkString(", ")


    val sqlQuery = s"""
      |MERGE INTO $coreTable target
      |USING incoming_records source
      |ON $joinString
      |WHEN MATCHED THEN
      |  UPDATE SET $updateString
      |WHEN NOT MATCHED
      |  THEN INSERT *
      """.stripMargin
    sparkSession.sql(sqlQuery)
  }

  private def getCoreDF(sinkPath: String, partitionCol: String, partitionsToUpdate: Array[String], TargetSchema: StructType)
                       (implicit sparkSession: SparkSession) : DataFrame = {

    val sinkDF = try{
      sparkSession.read.parquet(sinkPath)
        .filter(col(partitionCol).isin(partitionsToUpdate:_*))
    }  catch {
      case e: org.apache.spark.sql.AnalysisException if e.getMessage().contains("UNABLE_TO_INFER_SCHEMA") =>
        logger.warn(s"Silver path $sinkPath is empty. Initializing empty DataFrame.")
        sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], TargetSchema)
      case e: Exception => throw e
    }
    sinkDF
  }
}
