package com.olist.silver.common

import com.olist.silver.common.utils.{ConfigLoader, PipelineConfig}
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
    val config: PipelineConfig = ConfigLoader.load(configPath)
    logger.info("Creating sparkSession")

    implicit val sparkSession: SparkSession = {
      config.runEnv match {
        case "local" => SparkSession.builder()
          .appName(config.appName)
          .config(config.sparkOptions)
          .master("local[*]")
          .getOrCreate()

        case _ => SparkSession.builder()
          .appName(config.appName)
          .config(config.sparkOptions)
          .getOrCreate()
      }
    }

    sparkSession.sparkContext.setLogLevel("WARN")

    try {
      run(config)
    }
    finally sparkSession.stop()
  }

  def run(config: PipelineConfig)(implicit sparkSession: SparkSession) : Unit
}
