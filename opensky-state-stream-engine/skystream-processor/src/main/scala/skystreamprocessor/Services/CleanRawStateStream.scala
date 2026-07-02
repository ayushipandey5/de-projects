package skystreamprocessor.Services

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes.stringSerde
import java.util.Properties

object CleanRawStateStream {

  protected val logger : Logger = LogManager.getLogger(this.getClass)
  def main(args : Array[String]) : Unit = {
    val appName = "skystream-plumber-v1"
    val ingest_topic = "opensky_raw_vectors"
    val brokers = "localhost:19092,localhost:29092,localhost:39092"
    val write_topic ="opensky_clean_vectors"

    val props = new Properties()

    props.put(StreamsConfig.APPLICATION_ID_CONFIG,appName)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,brokers)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.Serdes.StringSerde])
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.Serdes.StringSerde])
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val builder = new StreamsBuilder()

    val rawFlightStreams = builder.stream[String,String](ingest_topic)

    val cleanFlightStream = rawFlightStreams.flatMapValues{ value =>
      //      println(s"\n[DEBUG RAW VALUE] -> $value")
      try{
        //        println(s"value :::::  $value")
        val arr = ujson.read(value).arr
        //        println(s"arr :::::  $arr")

        if(arr(5).isNull || arr(6).isNull){
          //          println("[DEBUG] Dropped: Missing Coordinates")
          Iterable.empty
        } else {
          val cleanObj = ujson.Obj(
            "icao24" -> arr(0).str,
            "callsign" -> (if (arr(1).isNull) "UNKNOWN" else arr(1).str.trim),
            "origin_country" -> arr(2).str,
            "last_contact" -> arr(4).num,
            "longitude" -> arr(5).num,
            "latitude" -> arr(6).num,
            "vertical_rate" -> arr(11).num,
            "geo_altitude" -> arr(13).num,
            "velocity" -> (if (arr(9).isNull) 0.0 else arr(9).num),
            "on_ground" -> arr(8).bool,
            "category" -> ( if (arr.arr.length <= 17 || arr(17).isNull) {"NO information provided"}
            else{
              arr(17).num.toInt match{
                case 0 => "No information at all"
                case 1 => "No ADS-B Emitter Category Information"
                case 2 => "Light (< 15500 lbs)"
                case 3 => "Small (15500 to 75000 lbs)"
                case 4 => "Large (75000 to 300000 lbs)"
                case 5 => "High Vortex Large"
                case 6 => "Heavy (> 300000 lbs)"
                case 7 => "High Performance (> 5g and > 400 knots)"
                case 8 => "Rotorcraft"
                case 9 => "Glider / Sailplane"
                case 10 => "Lighter-than-air"
                case 14 => "UAV"
                case 20 => "Space / Trans-atmospheric vehicle"
                case other => s"Unknown Code: $other"
              }
            })
          )
          //          println(s"cleanObj :::::  $cleanObj")

          Iterable(ujson.write(cleanObj))
        }
      } catch {
        case e: Exception => println(s"[CRITICAL PARSE ERROR] Exception: ${e.getClass.getSimpleName} - ${e.getMessage}")
          Iterable.empty
      }
    }

    cleanFlightStream.peek { (k,v) =>
      logger.info(s"[Clean Vector] $k -> $v")
    }

    cleanFlightStream.to(write_topic)

    val topology = builder.build()

    val streams = new KafkaStreams(topology, props)

    sys.ShutdownHookThread {
      logger.info("Shutting down Plumber topology...")
      streams.close()
    }

    logger.info("Day 1 Topology Started. Processing raw vectors into clean JSON objects...")
    streams.start()
  }
}
