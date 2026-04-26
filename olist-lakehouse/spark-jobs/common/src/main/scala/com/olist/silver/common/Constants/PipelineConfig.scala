package com.olist.silver.common.Constants

case class SourceConfig(
                        dataPath : String,
                        checkPointPath: String,
                        partitionColumn: String,
                        maxPartitionsToRead : Int,
                        fileFormat : String
                       )
case class SinkConfig(
                     dataPath : String
                     )

case class PipelineConfig(
                          appName: String,
                          runEnv : String,
                          gcpProjectId : Option[String],
                          serviceAccountPath : Option[String],
                          source: SourceConfig,
                          sink : SinkConfig,
                          sparkOptions : Map[String, String]
                         )
