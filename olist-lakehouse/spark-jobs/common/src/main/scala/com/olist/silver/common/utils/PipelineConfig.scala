package com.olist.silver.common.utils

case class SourceConfig(
                        dataPath : String,
                        checkPointPath: String,
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
