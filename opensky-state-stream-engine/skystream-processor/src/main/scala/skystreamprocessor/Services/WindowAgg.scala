package skystreamprocessor.Services

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{SessionWindows, SlidingWindows, TimeWindows}
import org.apache.logging.log4j.{LogManager, Logger}
import skystreamprocessor.Constants.FlightStats
import upickle.default._

import java.time.Duration
import java.util.Properties

// --- THE CRITICAL SCALA API IMPORTS ---
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._


object WindowAgg {
  protected val logger : Logger = LogManager.getLogger(this.getClass)
  def main(args : Array[String]) : Unit = {
    val appName = "tumbling-analyst-v1"
    val brokers = "localhost:19092,localhost:29092,localhost:39092"
    val read_topic = "opensky_clean_vectors"

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,classOf[FlightTimestampExtractor].getName)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.Serdes.StringSerde])
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.Serdes.StringSerde])
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")

    val builder = new StreamsBuilder()

    val cleanVectorStream = builder.stream[String,String](read_topic)

    val flightVelocityStream = cleanVectorStream
      .map{ (key,jsonVal) =>
        val value = ujson.read(jsonVal)
        val velocity = value("velocity").num
        (key,velocity)
      }

    val windowDuration = Duration.ofMinutes(5)
    val gracePeriod = Duration.ofMinutes(1)

//    TUMBLING WINDOW
    val tumblingWindow = TimeWindows.ofSizeAndGrace(windowDuration,gracePeriod)

//    HOPPING WINDOW
    val hop = Duration.ofMinutes(2)
    val hoppingWindow = TimeWindows
      .ofSizeAndGrace(windowDuration,gracePeriod)
      .advanceBy(hop)

//    SLIDING WINDOW
    val slidingWindow = SlidingWindows.ofTimeDifferenceAndGrace(windowDuration,gracePeriod)

//    SESSION WINDOWS
    val sessionGap = Duration.ofMinutes(10)
    val sessionWindow = SessionWindows.ofInactivityGapWithNoGrace(sessionGap)

    implicit val flightStatsRW : ReadWriter[FlightStats] = macroRW[FlightStats]

    val flightStatsSerializer = new Serializer[FlightStats] {
      override def serialize(topic: String, data: FlightStats): Array[Byte] = {
        if(data == null) null
        else write(data).getBytes
      }
    }

    val  flightStatsDeserializer = new Deserializer[FlightStats] {
      override def deserialize(topic: String, data: Array[Byte]): FlightStats = {
        if(data == null) null.asInstanceOf[FlightStats]
        else read[FlightStats](data)
      }
    }

    implicit val flightStatsSerde : Serde[FlightStats] = Serdes.serdeFrom(flightStatsSerializer,flightStatsDeserializer)

    val windowedVeclocityTable = flightVelocityStream
      .groupByKey
      .windowedBy(tumblingWindow)
      .aggregate(FlightStats(0L,0.0))(
        (_, velocity, stats) =>
          FlightStats(
            count = stats.count + 1,
            totalVelocity = stats.totalVelocity + velocity
          ))(
        Materialized.as("tumbling-stats-store")
      )

    windowedVeclocityTable.toStream.peek{ (windowedKey,stats) =>
      println(s"key => $windowedKey")
      println(s"stats => $stats")
      val icao = windowedKey.key()
      val start = windowedKey.window().start()
      val avg = if (stats.count > 0) stats.totalVelocity/stats.count else 0.0
      logger.info(s"[WINDOW $start] Aircraft $icao | Avg Velocity: $avg m/s | Data Points: ${stats.count}")
    }

    val topology = builder.build()

    val streams = new KafkaStreams(topology,props)

    sys.ShutdownHookThread{
      logger.info("Shutting down Tumbling Analyst...")
      streams.close()
    }

    logger.info("Starting Analyst Topology...")
    streams.start()

  }
}
