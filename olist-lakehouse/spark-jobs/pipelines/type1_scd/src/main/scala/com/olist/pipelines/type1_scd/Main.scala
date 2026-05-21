package com.olist.pipelines.type1_scd

import com.olist.pipelines.type1_scd.Constants.ColumnConstants._
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
    val transformedDF = Transformer.execute(rawDF,config.appName)
    val (dedupPartitionByCols, dedupOrderByCols, targetSchema) = config.appName match {
      case name if name.contains("customers") => (dedupCustomersPartitionByCols, dedupCustomersOrderByCols, CustomersTargetSchema)
      case name if name.contains("sellers") => (dedupSellersPartitionByCols, dedupSellersOrderByCols, SellersTargetSchema)
      case name if name.contains("products") => (dedupProductsPartitionByCols, dedupProductsOrderByCols, ProductsTargetSchema)
    }
    val sinkDF = UpsertHelper.execute(transformedDF,config.sink.dataPath, config.sink.partitionColumn,dedupPartitionByCols, dedupOrderByCols, targetSchema)
    ReadWriteHelper.writeToGCS(sinkDF,config.sink.dataPath,"overwrite",config.sink.partitionColumn)
    DataProcessingHelper.updateCheckPoint(maxProcessedPartition,config.source.checkPointPath)
  }


}