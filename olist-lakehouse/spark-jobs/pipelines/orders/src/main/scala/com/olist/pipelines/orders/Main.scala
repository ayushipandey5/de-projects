package com.olist.pipelines.orders

import com.olist.pipelines.orders.Constants.ColumnConstants._
import com.olist.pipelines.orders.Services.Transformer
import com.olist.silver.common.Constants.PipelineConfig
import com.olist.silver.common.SparkJob
import com.olist.silver.common.Utils.{DataProcessingHelper, ReadWriteHelper, UpsertHelper}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main extends SparkJob{
  def main(args : Array[String]) : Unit = {
    execute(args)
  }

  override def runPipeline(config: PipelineConfig)(implicit sparkSession: SparkSession): Unit = {
    val (rawDF, maxProcessedPartition) = ReadWriteHelper.readFromSource(config)
    val transformedDF = Transformer.execute(rawDF)
    val reorderedDF = DataProcessingHelper.selectAndReorder(transformedDF,TargetSceham)
    val stagingDF = UpsertHelper.execute(reorderedDF.filter(col(config.source.partitionColumn).isNotNull) ,
      config.sink.dataPath,
      config.source.partitionColumn,
      partitionByCols = dedupePartitionCols,
      dedupeOrderByCols,
      TargetSceham
    )
    ReadWriteHelper.writeToGCS(stagingDF,config.sink.dataPath,"overwrite",config.source.partitionColumn)
    DataProcessingHelper.updateCheckPoint(maxProcessedPartition,config.source.checkPointPath)
  }

}
