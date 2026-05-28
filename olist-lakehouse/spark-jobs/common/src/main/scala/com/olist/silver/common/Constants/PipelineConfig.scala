package com.olist.silver.common.Constants

sealed trait PipelineConfig {
  def appName : String
  def sparkOptions : Map[String,String]
  def runEnv : String
  def source : SourceConfig
  def sink : SinkConfig
}

sealed trait SinkConfig {
  def dataPath: String
  def tableName : String
  def partitionColumn : String
}


case class SourceConfig(
                         dataPath : String,
                         checkPointPath: String,
                         partitionColumn: String,
                         maxPartitionsToRead : Int,
                         fileFormat : String
                       )
case class StandardSinkConfig(
                       dataPath : String,
                       partitionColumn : String,
                       tableName : String
                     ) extends SinkConfig

case class BigLakeSinkConfig(
                       catalogName : String,
                       tableName : String,
                       dataPath : String,
                       partitionColumn : String
                     ) extends SinkConfig

case class StandardConfig(
                           appName: String,
                           runEnv : String,
                           gcpProjectId : Option[String],
                           serviceAccountPath : Option[String],
                           source: SourceConfig,
                           sink : StandardSinkConfig,
                           sparkOptions : Map[String, String]
                         ) extends PipelineConfig


case class IcebergPipelineConfig(
                                  appName : String,
                                  runEnv : String,
                                  gcpProjectId : Option[String],
                                  serviceAccountPath : Option[String],
                                  source: SourceConfig,
                                  sink : BigLakeSinkConfig,
                                  sparkOptions : Map[String, String]
                                ) extends PipelineConfig
