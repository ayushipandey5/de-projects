package skystreamprocessor.services

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes.stringSerde

import java.util.Properties

object FlightTracker {
  protected val logger : Logger = LogManager.getLogger(this.getClass)
  def main(args:Array[String]) : Unit = {
    val read_topic = "opensky_clean_vectors"
    val appName = "skystream-flight-tracker-v1"
    val brokers = "localhost:19092,localhost:29092,localhost:39092"

    val props = new Properties()

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.Serdes.StringSerde])
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.Serdes.StringSerde])
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")

    val builder = new StreamsBuilder()

    val cleanFlightStream : KStream[String,String] = builder.stream[String,String](read_topic)

    val activeFlightsTable : KTable[String,String] = cleanFlightStream
      .groupByKey
      .reduce((oldState,newState) => newState)

    activeFlightsTable.toStream.peek{(k,v)=>
      logger.info(s"[STATE UPDATED] Aircraft $k is now at: $v")
    }

    val topology = builder.build()
    val streams = new KafkaStreams(topology,props)

    sys.ShutdownHookThread {
      println("Shutting down Tracker topology...")
      streams.close()
    }

    println("Day 2 Tracker Started. Materializing active flights view in RocksDB...")
    streams.start()

  }

}
