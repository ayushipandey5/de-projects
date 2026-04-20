package com.olist.pipelines.customers

import com.olist.silver.common.SparkJob
import com.olist.silver.common.utils.PipelineConfig
import org.apache.spark.sql.SparkSession



object Main extends SparkJob {
  def main(args : Array[String]) : Unit = {
    execute(args)
  }
  override def run(config: PipelineConfig)(implicit sparkSession: SparkSession): Unit = {

  }

}