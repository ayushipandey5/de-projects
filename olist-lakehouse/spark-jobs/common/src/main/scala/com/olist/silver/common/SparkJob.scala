package com.olist.silver.common

import com.olist.silver.common.Constants.PipelineConfig
import com.olist.silver.common.Utils.ConfigLoader
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

trait SparkJob {
  protected lazy val logger: Logger = LogManager.getLogger(this.getClass.getName)

  def execute(args: Array[String]): Unit = {
    logger.info("Getting application configs..")
    //     each pipeline will have a config file passed containing some info like what is run env
    val configPath = Option(System.getProperty("appConfig")).getOrElse {
      throw new RuntimeException("Missing JVM flag for appConfig")
    }
//    val config: PipelineConfig = ConfigLoader.load(configPath)
    val runType = Option(System.getProperty("runType")).getOrElse {
      throw new RuntimeException("Missing JVM flag for runType")
    }
    val config: PipelineConfig = runType match {
      case "iceberg" => ConfigLoader.loadIcebergConfig(configPath)
      case _ => ConfigLoader.load(configPath)
    }
    logger.info("Creating sparkSession")


    implicit val sparkSession: SparkSession = {
      val builder = SparkSession.builder().appName(config.appName)
      config.sparkOptions.foreach{case (key,value) =>
      builder.config(key,value)}
      builder.getOrCreate()
    }
    sparkSession.sparkContext.setLogLevel("WARN")

    try {
      runPipeline(config)
    }
    finally sparkSession.stop()
  }

  def runPipeline(config: PipelineConfig)(implicit sparkSession: SparkSession) : Unit
}
