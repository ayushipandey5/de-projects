package com.olist.silver.common.Utils

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.iceberg.spark.actions.SparkActions

object DataProcessingHelper {
  private lazy val logger:Logger = LogManager.getLogger(this.getClass.getName)

  def selectAndReorder(inputDF:DataFrame, targetSchema: StructType) : DataFrame = {
    val selectExpression = targetSchema.fieldNames.map{ fieldName =>
      if(inputDF.columns.contains(fieldName)){
        col(fieldName).cast(targetSchema(fieldName).dataType)
      }
      else{
        lit(null).cast(targetSchema(fieldName).dataType).as(fieldName)
      }
    }
    inputDF.select(selectExpression:_*)
  }

  def compaction(tableName:String) (implicit sparkSession: SparkSession) : Unit = {
    val icebergTable = org.apache.iceberg.spark.Spark3Util
      .loadIcebergTable(sparkSession,tableName)

    //    Compact small files into large, healthy sequential blocks - weekly or after massive raw batch runs
    val compactionResult = SparkActions.get(sparkSession)
      .rewriteDataFiles(icebergTable)
      .option("target-file-size-bytes", (128 * 1024 * 1024).toString  )
      .execute()

    // Expire snapshots older than 3 days
    //// This removes old metadata files and tells GCS it can actually delete the old Parquet fragments
    val expireSnapshotResult = SparkActions.get(sparkSession)
      .expireSnapshots(icebergTable)
      .expireOlderThan(System.currentTimeMillis() - (3 * 24 * 60 * 60 * 1000))
      .retainLast(5)
      .execute()
  }

  def updateCheckPoint (maxProcessedPartition : String, checkPointPath : String)
                               (implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    logger.info(s"Updating checkpoint : ${maxProcessedPartition}")
    val checkPointDF = List(maxProcessedPartition).toDF("value")
    checkPointDF.write
      .mode("overwrite")
      .text(checkPointPath)
  }


}
