package skystreamprocessor

import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes.stringSerde

import java.util.Properties

object Main {
  protected val logger : Logger = LogManager.getLogger(this.getClass)
  def main(args : Array[String]) : Unit = {

  }
}
