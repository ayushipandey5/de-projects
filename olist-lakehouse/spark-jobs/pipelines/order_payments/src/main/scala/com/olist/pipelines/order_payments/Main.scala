package com.olist.pipelines.order_payments

import com.olist.pipelines.order_payments.Constants._
import com.olist.silver.common.Constants.PipelineConfig
import com.olist.silver.common.SparkJob
import com.olist.silver.common.Utils.{DataProcessingHelper, ReadWriteHelper}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object Main extends SparkJob{
  def main(args : Array[String]) : Unit = {
    execute(args)
  }

  override def runPipeline(config: PipelineConfig)(implicit sparkSession: SparkSession): Unit = {
    val (rawDF, maxProcessedPartition) = ReadWriteHelper.readFromSource(config)
    rawDF.cache()
    val windowSpec = Window.partitionBy(dedupePartitionCols:_*).orderBy(dedupeOrderByCols:_*)
    val dedupedDF = rawDF.withColumn("rn", row_number().over(windowSpec))
      .filter(col("rn") === 1)
      .drop(col("rn"))
      .withColumn("event_d",col("prcs_d").cast(DateType))
    val sinkDF = DataProcessingHelper.selectAndReorder(dedupedDF,TargetSchema)
    ReadWriteHelper.writeToGCS(sinkDF,config.sink.dataPath,"overwrite","event_d")
    DataProcessingHelper.updateCheckPoint(maxProcessedPartition,config.source.checkPointPath)
  }
}