package com.olist.silver.common.Constants

case class SourceConfig(
                        dataPath : String,
                        checkPointPath: String,
                        partitionColumn: String,
                        processDate : Int
                       )
case class SinkConfig(
                     dataPath : String
                     )

case class PipelineConfig(
                          appName: String,
                          runEnv : String,
                          gcpProjectId : String,
                          serviceAccountPath : String,
                          source: SourceConfig,
                          sink : SinkConfig,
                          sparkOptions : Map[String, String]
                         )
