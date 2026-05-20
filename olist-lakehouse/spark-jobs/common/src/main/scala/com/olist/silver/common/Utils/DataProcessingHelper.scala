package com.olist.silver.common.Utils

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

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
