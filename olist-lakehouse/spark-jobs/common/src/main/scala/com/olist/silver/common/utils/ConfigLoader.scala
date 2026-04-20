package com.olist.silver.common.utils

import pureconfig._
import pureconfig.generic.auto._

object ConfigLoader {
  def load(path: String): PipelineConfig = {
    ConfigSource.file(path).load[PipelineConfig] match {
      case Left(failures) =>
          throw new Exception(s"Failed to load config : ${failures.toList.mkString(",")}")
      case Right(config) => config
    }
  }
}
