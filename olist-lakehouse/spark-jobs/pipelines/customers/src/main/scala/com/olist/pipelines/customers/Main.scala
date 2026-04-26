package com.olist.pipelines.customers

import com.olist.pipelines.customers.Constants.ColumnConstants.{orderByCols, partitionByCols}
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
    logger.info(s"Max processed partition : ${maxProcessedPartition}")
    val transformedDF = Transformer.execute(rawDF)
    val sinkData = UpsertHelper.execute(transformedDF,config.source.dataPath, config.source.partitionColumn,partitionByCols, orderByCols)

  }

}