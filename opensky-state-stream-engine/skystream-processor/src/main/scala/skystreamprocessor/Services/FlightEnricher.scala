package skystreamprocessor.Services


import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes._

import java.util.Properties
import scala.util.Try

object FlightEnricher {
  protected val logger : Logger = LogManager.getLogger(this.getClass)

  def main(args : Array[String]) : Unit = {
    logger.info("Starting Flight Enricher...")

    val appName = "flight_enricher"
    val read_stream = "opensky_clean_vectors"
    val read_table = "aircraft_registry"
    val brokers = "localhost:19092,localhost:29092,localhost:39092"
    val write_topic = "aircraft_enriched"

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val builder = new StreamsBuilder()

    val cleanVectorStream: KStream[String,String] = builder.stream[String,String](read_stream)
    val registryTable : KTable[String, String] = builder.table[String,String](read_table)

////  Inner JOIN
//    val enrichedStream : KStream[String,String] = cleanVectorStream.join(registryTable)(
////      JOINER : (StreamValue,TableValue) => NewValue
//      (cleanVectorStreamStr,registryTableStr) => {
//        try{
//          val cleanVectorObj = ujson.read(cleanVectorStreamStr).obj
//          val registryObj = ujson.read(registryTableStr).obj
//
//          val manufacturer = registryObj.getOrElse("manufacturer", ujson.Str("Unknown")).str
//          val model = registryObj.getOrElse("model", ujson.Str("Unknown")).str
//          val airline = registryObj.getOrElse("airline", ujson.Str("Unknown")).str
//
//          cleanVectorObj("aircraft_manufacturer") = manufacturer
//          cleanVectorObj("aircraft_model") = model
//          cleanVectorObj("airline") = airline
//
//          ujson.write(cleanVectorObj)
//        } catch {
//          case e :Exception =>
//            logger.warn(s"Failed to join payloads: ${e.getMessage}")
//            cleanVectorStreamStr
//        }
//      }
//    )

//    LEFT JOin

    val enrichedStream : KStream[String,String] = cleanVectorStream.leftJoin(registryTable)(
      (cleanVectorJson, registryTableJson) =>
        try{
          val cleanVectorObj = ujson.read(cleanVectorJson).obj
          val registryTableObj = Try(ujson.read(registryTableJson).obj).getOrElse(ujson.Obj().obj)

          val manufacturer = Try(registryTableObj("manufacturer")).getOrElse(ujson.Str("Unknown"))
          val model = Try(registryTableObj("model")).getOrElse(ujson.Str("Unknown"))
          val airline = Try(registryTableObj("airline")).getOrElse(ujson.Str("Unknown"))

          cleanVectorObj("airline_manufacturer") = manufacturer
          cleanVectorObj("aircraft_model") = model
          cleanVectorObj("airline") = airline
          ujson.write(cleanVectorObj)
        }catch {
          case e:Exception =>
            logger.error(s"Error in joining ${e.getMessage}")
            cleanVectorJson
        }
    )

    enrichedStream.peek{ (k,v) =>
      logger.info(s"[ENRICHED] $k -> $v")
    }.to(write_topic)

    val topology = builder.build
    val streams = new KafkaStreams(topology,props)


    sys.ShutdownHookThread{
      logger.info("Shutting down Enricher Topology..")
      streams.close()
    }

    logger.info("Enricher Started. Waiting for matching keys in both topics...")
    streams.start()
    
  }
}
