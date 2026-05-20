package com.olist.pipelines.type1_scd

import com.olist.pipelines.type1_scd.Constants.ColumnConstants.{TargetSchema, orderByCols, partitionByCols}
import com.olist.pipelines.type1_scd.Services.Transformer
import com.olist.silver.common.Constants.PipelineConfig
import com.olist.silver.common.SparkJob
import org.apache.spark.sql.SparkSession
import com.olist.silver.common.Utils.{ConfigLoader, DataProcessingHelper, ReadWriteHelper, UpsertHelper}



object Main extends SparkJob {
  def main(args : Array[String]) : Unit = {
    execute(args)
  }
  override def runPipeline(config: PipelineConfig)(implicit sparkSession: SparkSession): Unit = {
    val (rawDF, maxProcessedPartition) = ReadWriteHelper.readFromSource(config)
    val transformedDF = Transformer.execute(rawDF)
    val sinkDF = UpsertHelper.execute(transformedDF,config.sink.dataPath, "state",partitionByCols, orderByCols, TargetSchema)
    ReadWriteHelper.writeToGCS(sinkDF,config.sink.dataPath,"overwrite","state")
    DataProcessingHelper.updateCheckPoint(maxProcessedPartition,config.source.checkPointPath)
  }


}