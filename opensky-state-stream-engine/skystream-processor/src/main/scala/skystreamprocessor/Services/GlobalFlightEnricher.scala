package skystreamprocessor.Services

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.serialization.Serdes._

import java.util.Properties
import scala.util.Try

object GlobalFlightEnricher {
  protected val logger : Logger = LogManager.getLogger(this.getClass)

  def main(args : Array[String]) : Unit = {
    val global_table = "global_registry"
    val read_stream = "opensky_clean_vectors"
    val brokers = "localhost:19092,localhost:29092,localhost:39092"
    val appName = "skystream-global-enricher-v1"
    val write_topic = "global_enriched_flights"

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG,appName)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val builder = new StreamsBuilder()

    val globalRegistryTable : GlobalKTable[String,String] = builder.globalTable[String,String](global_table)
    val cleanVectorStream : KStream[String,String] = builder.stream[String,String](read_stream)

    val globalEnrichedStream : KStream[String,String] = cleanVectorStream.leftJoin(globalRegistryTable)(
    (vectorKey,vectorJson) => {
      try{
        val vectorObj = ujson.read(vectorJson).obj
        vectorObj("icao24").str
      }catch {
        case _: Exception => vectorKey
      }
    },
      (vectorJson, registryJson)=>{
        try{
          val vectorObj = ujson.read(vectorJson).obj
          val registryObj = Try(ujson.read(registryJson).obj).getOrElse(ujson.Obj().obj)

          val manufacturer = Try(registryObj("manufacturer")).getOrElse(ujson.Str("Unknown"))
          val model = Try(registryObj("model")).getOrElse(ujson.Str("Unknown"))
          val airline = Try(registryObj("airline")).getOrElse(ujson.Str("Unknown"))

          vectorObj("airline_manufacturer") = manufacturer
          vectorObj("aircraft_model") = model
          vectorObj("airline") = airline
          ujson.write(vectorObj)
        }catch {
          case _:Exception => vectorJson
        }
      })

    globalEnrichedStream.peek{(k,v) => logger.info(s"[Enriched with Global registry $k -> $v]")}

    globalEnrichedStream.to(write_topic)

    val topology = builder.build()
    val streams = new KafkaStreams(topology,props)

    sys.ShutdownHookThread{
      logger.info("Shutting down Global Flight Enricher...")
      streams.close()
    }

    logger.info("Global Enricher started. Waiting for messages to match...")
    streams.start()

  }
}
