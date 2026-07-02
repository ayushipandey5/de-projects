package skystreamprocessor.Services

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._

import java.time.{Duration, Instant, ZoneId}
import java.util.Properties
import scala.util.Try

object FeatureVector {
  protected  val logger: Logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]) : Unit = {
    val appName = "feature_extraction"
    val state_topic = "opensky_clean_vectors"
    val flights_topic = "raw_flights"
    val bootstrap_servers = "localhost:19092,localhost:29092,localhost:39092"

    val write_topic = "features_vector"

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_servers)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    val builder = new StreamsBuilder()
    val stateStream : KStream[String,String] = builder.stream[String,String](state_topic)
    val flightsStream : KStream[String,String] = builder.stream[String,String](flights_topic)

    val windowSize = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(3))

    val features_vector_stream : KStream[String,String] = flightsStream.join(stateStream)(
      (flightJson,stateJson) =>
      try{
        val flightObj = ujson.read(flightJson).obj
        val stateObj = ujson.read(stateJson).obj

        val lastContact = Try(stateObj("last_contact").num.toLong).getOrElse(0L)
        val firstSeen = Try(flightObj("firstSeen").num.toLong).getOrElse(0L)

        val zonedDateTime = Instant.ofEpochSecond(lastContact).atZone(ZoneId.of("UTC"))
        val distanceKm = Try(flightObj("estArrivalAirportHorizDistance").num / 1000.0).getOrElse(-1.0)

        val featureObj = ujson.Obj(
          "icao24" -> flightObj.getOrElse("icao24", ujson.Str("Unknown")),
          "timestamp" -> lastContact,

          "intent_features" -> ujson.Obj(
            "departure_airport" -> flightObj.getOrElse("estDepartureAirport", ujson.Str("Unknown")),
            "arrival_airport" -> flightObj.getOrElse("estArrivalAirport", ujson.Str("Unknown"))
          ),

          "kinematic_features" -> ujson.Obj(
            "altitude" -> stateObj.getOrElse("geo_altitude", ujson.Num(-1)),
            "velocity" -> stateObj.getOrElse("velocity", ujson.Num(-1)),
            "vertical_rate" -> stateObj.getOrElse("vertical_rate", ujson.Num(0))
          ),

          "engineered_temporal_features" -> ujson.Obj(
            "seconds_since_departure" -> (if (lastContact > 0 && firstSeen > 0) lastContact - firstSeen else -1),
            "hour_of_day" -> zonedDateTime.getHour,
            "day_of_week" -> zonedDateTime.getDayOfWeek.getValue
          ),

          "engineered_spatial_features" -> ujson.Obj(
            "current_latitude" -> stateObj.getOrElse("latitude", ujson.Num(0.0)),
            "current_longitude" -> stateObj.getOrElse("longitude", ujson.Num(0.0)),
            "distance_to_dest_km" -> distanceKm
          )
        )
        ujson.write(featureObj)
      }catch {
        case e:Exception =>
          logger.error(s"Failed to engineer features: ${e.getMessage}")
          null
      },
      windowSize
    )

    features_vector_stream.filterNot((key, value) => value == null).to(write_topic)

    val topology = builder.build
    val stream = new KafkaStreams(topology,props)

    sys.ShutdownHookThread{
      logger.info("Shutting down feature extraction...")
      stream.close()
    }

    logger.info("Starting feature extraction...")
    stream.start()

  }
}
