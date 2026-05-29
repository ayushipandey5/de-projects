package com.olist.pipelines.reference_dimensions

import com.olist.pipelines.reference_dimensions.Constants.ColumnConstants._
import com.olist.pipelines.reference_dimensions.Services.Transformer
import com.olist.silver.common.Constants.PipelineConfig
import com.olist.silver.common.SparkJob
import com.olist.silver.common.Utils
import com.olist.silver.common.Utils.{DataProcessingHelper, ReadWriteHelper}
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

object Main extends SparkJob {
  def main(args : Array[String]) : Unit = {
    execute(args)
  }
  override def runPipeline(config: PipelineConfig)(implicit sparkSession: SparkSession): Unit = {
    val rawDF = sparkSession.read.parquet(config.source.dataPath)
    val transformedDF = Transformer.execute(rawDF,config.appName)
    val targetSchema = config.appName match {
      case name if name.contains("geolocation") =>
        ReadWriteHelper.createIcebergTableWithSchema(config.sink.tableName,geolocationTargetSchema,Seq(),"",geolocationTableProperties)
        geolocationTargetSchema
      case name if name.contains("category") => productCatTransTargetSchema
    }
    val sinkDF = DataProcessingHelper.selectAndReorder(transformedDF,targetSchema)
//    ReadWriteHelper.writeToGCS(sinkDF,config.sink.dataPath,"overwrite","")
    ReadWriteHelper.writeToIcebergTable(sinkDF,config.sink.tableName,"overwrite","")
  }


}