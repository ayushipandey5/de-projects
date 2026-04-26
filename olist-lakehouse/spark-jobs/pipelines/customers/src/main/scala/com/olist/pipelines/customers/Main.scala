package com.olist.pipelines.customers

import com.olist.pipelines.customers.Constants.ColumnConstants.{TargetSchema, orderByCols, partitionByCols}
import com.olist.pipelines.customers.Services.Transformer
import com.olist.silver.common.Constants.PipelineConfig
import com.olist.silver.common.SparkJob
import org.apache.spark.sql.SparkSession
import com.olist.silver.common.Utils.{ConfigLoader, ReadWriteHelper, UpsertHelper}



object Main extends SparkJob {
  def main(args : Array[String]) : Unit = {
    execute(args)
  }
  override def runPipeline(config: PipelineConfig)(implicit sparkSession: SparkSession): Unit = {
    val (rawDF, maxProcessedPartition) = ReadWriteHelper.readFromSource(config)
    val transformedDF = Transformer.execute(rawDF)
    val sinkDF = UpsertHelper.execute(transformedDF,config.sink.dataPath, "state",partitionByCols, orderByCols, TargetSchema)
    ReadWriteHelper.writeToGCS(sinkDF,config.sink.dataPath, "state")
    updateCheckPoint(maxProcessedPartition,config.source.checkPointPath)
  }
  private def updateCheckPoint (maxProcessedPartition : String, checkPointPath : String)
                               (implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    logger.info(s"Updating checkpoint : ${maxProcessedPartition}")
    val checkPointDF = List(maxProcessedPartition).toDF("value")
    checkPointDF.write
      .mode("overwrite")
      .text(checkPointPath)
  }

}