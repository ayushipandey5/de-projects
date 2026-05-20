package com.olist.pipelines.reference_dimensions

import com.olist.pipelines.reference_dimensions.Services.Transformer
import com.olist.silver.common.Constants.PipelineConfig
import com.olist.silver.common.SparkJob
import com.olist.silver.common.Utils
import com.olist.silver.common.Utils.ReadWriteHelper
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

object Main extends SparkJob{
  def main(args : Array[String]) : Unit = {
    execute(args)
  }

  override def runPipeline(config: PipelineConfig)(implicit sparkSession : SparkSession) : Unit = {
    val (rawDF, maxProcessedPartition) = ReadWriteHelper.readFromSource(config)
    val transformedDF = Transformer.execute(rawDF)

  }

}
