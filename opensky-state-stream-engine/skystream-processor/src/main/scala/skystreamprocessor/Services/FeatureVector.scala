package skystreamprocessor.Services

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, StreamJoined}
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._

import java.time.{Duration, Instant, ZoneId}
import java.util.{Collections, Properties}
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

    val schemaRegistryUrl = "http://localhost:18081"
    val serdeConfig = Collections.singletonMap(
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
      schemaRegistryUrl
    )

    val avroSerde = new GenericAvroSerde()
    avroSerde.configure(serdeConfig,false)

    val builder = new StreamsBuilder()
    val stateStream : KStream[String,String] = builder.stream[String,String](state_topic)
    val flightsStream = builder.stream(flights_topic)(
      Consumed.`with`(stringSerde,avroSerde)
    )

    val windowSize = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(3))
    val streamJoinParam = StreamJoined.`with`(
      stringSerde,
      avroSerde,
      stringSerde
    )

    val features_vector_stream = flightsStream.join(stateStream)(
      (flightRecord:GenericRecord, stateJson) =>
      try{
        val stateObj = ujson.read(stateJson).obj

        val lastContact = Try(stateObj("last_contact").num.toLong).getOrElse(0L)
        val firstSeen = flightRecord.get("firstSeen").asInstanceOf[Long]

        val zonedDateTime = Instant.ofEpochSecond(lastContact).atZone(ZoneId.of("UTC"))
        val distanceKm = flightRecord.get("estArrivalAirportHorizDistance").asInstanceOf[Double] / 1000.0

        val depAirport = Option(flightRecord.get("estDepartureAirport")).map(_.toString).getOrElse("Unknown")
        val arrAirport = Option(flightRecord.get("estArrivalAirport")).map(_.toString).getOrElse("Unknown")

        val featureObj = ujson.Obj(
          "icao24" -> flightRecord.get("icao24").toString,
          "timestamp" -> lastContact,

          "intent_features" -> ujson.Obj(
            "departure_airport" -> depAirport,
            "arrival_airport" -> arrAirport
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
    )(streamJoinParam)

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
