package com.olist.silver.common.Utils

import com.olist.silver.common.Constants.PipelineConfig
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}


object ReadWriteHelper {
  protected lazy val logger : Logger = LogManager.getLogger(this.getClass.getName)

  def readFromSource(config : PipelineConfig)
                    (implicit sparkSession: SparkSession) : (DataFrame, String) = {
    val checkPointPath: String = config.source.checkPointPath
    val sourcePath : String = config.source.dataPath
    val partitionColumn: String = config.source.partitionColumn
    val maxPartitionsToRead : Int = config.source.maxPartitionsToRead
    val runEnv : String = config.runEnv
    val fileFormat : String = config.source.fileFormat
    val checkPointVal : String = getCheckPointValue(checkPointPath).getOrElse("1900-01-01")
    val lastProcessedDate = if(checkPointVal.length >= 10)  checkPointVal.substring(0,10) else "1900-01-01"

    val validPartitions = listValidPartitions(sourcePath,lastProcessedDate)
      .sorted
      .take(maxPartitionsToRead)

    if(validPartitions.isEmpty) {
      logger.warn(s"No new partitions found after $lastProcessedDate")
      return (sparkSession.emptyDataFrame,checkPointVal)
    }

    val readDF : DataFrame = fileFormat match {
      case "parquet" => sparkSession.read.parquet(validPartitions:_*)
      case "csv" => sparkSession.read.csv(validPartitions:_*)
      case _ => throw new IllegalArgumentException(s"Format $fileFormat not supported")
    }
    (readDF,validPartitions.last)
  }



  private def listValidPartitions (sourcePath : String,
                                   lastProcessedDate: String)
                                  (implicit sparkSession: SparkSession) : List[String] ={
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(new Path(sourcePath).toUri, conf)
    val statuses = fs.listStatus(new Path(sourcePath))

    statuses.filter(_.isDirectory)
      .map(_.getPath)
      .filter(_.getName.contains("="))
      .filter{ path =>
        val partitionVal = path.getName.split("=").last
      partitionVal > lastProcessedDate
      }
      .map(_.toString)
      .toList
  }

  private def getCheckPointValue(checkPointPath : String)(implicit sparkSession: SparkSession): Option[String] = {
    try {
      val checkPointVal = sparkSession.read.text(checkPointPath)
      if (checkPointVal.isEmpty) None
      else Some(checkPointVal.collectAsList().get(0).getString(0))
    } catch {
      case e: Exception =>
        logger.error(s"Failed to read checkpoint at $checkPointPath : ${e.getMessage}")
        None
    }

  }

}
